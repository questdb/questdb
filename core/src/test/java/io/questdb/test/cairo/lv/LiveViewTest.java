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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.lv.LiveViewCheckpointPurgeState;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshTask;
import io.questdb.cairo.lv.LiveViewRegistry;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.lv.LiveViewStateStore;
import io.questdb.cairo.lv.WalSegmentPageFrameCursor;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lifecycle and catalogue tests for live views. Complements
 * {@link LiveViewSmokeTest} (CREATE / DROP / refresh / restart / anchor reset)
 * with surface coverage:
 * <ul>
 *     <li>CREATE IF NOT EXISTS / DROP IF EXISTS idempotency.</li>
 *     <li>DROP of a non-existent live view raises an asserted-wording error.</li>
 *     <li>{@code tables()} reports the LV with table_type='L'.</li>
 *     <li>{@code information_schema.tables()} reports it as LIVE VIEW and not insertable.</li>
 *     <li>{@code pg_class()} reports relkind='v'.</li>
 *     <li>{@code SHOW COLUMNS} reflects the LV's projected schema, including the
 *     timestamp designation.</li>
 * </ul>
 */
public class LiveViewTest extends AbstractLiveViewTest {
    private static final String APPLY_RETRY_LINE = "live view has committed but unapplied WAL, retrying apply [view=lv";
    // Past FLUSH EVERY and past the 60 s flush-retry duration budget.
    private static final long PAUSE_PASS_MICROS = 61_000_000L;
    private static final String PAUSE_RN = "count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn";
    private static final int PAUSE_SHAPE_DEDUP = 1;
    private static final int PAUSE_SHAPE_LEAD = 0;
    // The rows the pause fixtures commit before a pause.
    private static final String PAUSED_ROWS = "(SELECT * FROM base WHERE wave = 0)";

    // One row per symbol value s<lo>..s<hi - 1>, a second apart from midnight.
    private static String insertDistinctSymbols(int lo, int hi) {
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, v) VALUES ");
        for (int i = lo; i < hi; i++) {
            if (i > lo) {
                sql.append(", ");
            }
            sql.append("('2026-01-01T00:00:").append(i < 10 ? "0" : "").append(i).append(".000000Z', 's")
                    .append(i).append("', ").append(i).append(')');
        }
        return sql.toString();
    }

    // One refresh pass with the clock moved past FLUSH EVERY and past the 60 s flush-retry budget, so a
    // paused view would flush, and a refused one would run its budget out, if anything drove it.
    private static boolean drivePausePass(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
        drainWalQueue();
        final boolean isWork = drainJob(job);
        drainWalQueue();
        return isWork;
    }

    private static void insertPauseRow(String ts, int x, int wave) throws SqlException {
        execute("INSERT INTO base (ts, x, wave) VALUES ('" + ts + "', " + x + ", " + wave + ")");
    }

    // Inserts (ts, x, wave) rows into base and into shadow, a table that gets every insert and no removal.
    private static void insertWithShadow(String values) throws SqlException {
        execute("INSERT INTO base (ts, x, wave) VALUES " + values);
        execute("INSERT INTO shadow (ts, x, wave) VALUES " + values);
    }

    private static LiveViewInstance lvInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private static SeqTxnTracker lvTracker() {
        return engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("lv"));
    }

    // The view SELECT of the pause fixtures over the rows of fromSql. Over a deduplicating base the same
    // SELECT takes the coupled applied-base drain instead of the lead path.
    private static String pauseViewSelect(String fromSql) {
        return "SELECT ts, x, " + PAUSE_RN + " FROM " + fromSql + " WHERE x > 0";
    }

    private static void suspendLiveViewWal(String withClause) {
        try {
            execute("ALTER LIVE VIEW lv SUSPEND WAL" + withClause);
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    // Pin the test clock below all test data before each test. A non-SEED
    // view's lower bound is the CREATE wall-clock moment, and the forward-append
    // refresh path drops rows below it. The test data is timestamped in the past,
    // so without a pinned clock the floor would land at real "now", above the
    // data, and every row would be dropped as pre-CREATE. currentMicros is a
    // static that leaks across tests, so pinning it here also makes the suite
    // order-independent. Tests that need a specific CREATE moment override this.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    private void assertAlterLiveViewRejected(String sql, String expectedMessageFragment) {
        try {
            execute(sql);
            Assert.fail("expected SqlException for " + sql);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "[sql=" + sql + "] expected message containing '" + expectedMessageFragment + "', got: " + e.getMessage(),
                    e.getMessage().contains(expectedMessageFragment)
            );
        }
    }

    private void assertRebaseWalReachesSuspensionCheck(TableToken token) {
        try {
            engine.rebaseWalTable(token);
            Assert.fail("REBASE WAL must still require suspension for a " + token.getType().keyword());
        } catch (CairoException e) {
            Assert.assertTrue(
                    "[kind=" + token.getType().keyword() + "] the kind guard must not fire, got: " + e.getFlyweightMessage(),
                    Chars.contains(e.getFlyweightMessage(), "REBASE WAL requires the table to be suspended first")
            );
        }
    }

    private void assertMutationRejected(String sql, String expectedMessageFragment) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try {
                execute(sql);
                Assert.fail("expected SqlException for " + sql);
            } catch (SqlException e) {
                Assert.assertTrue(
                        "expected message containing '" + expectedMessageFragment + "', got: " + e.getMessage(),
                        e.getMessage().contains(expectedMessageFragment)
                );
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    // Refreshes a live view whose residual filter is a SYMBOL equality, over commits that put the
    // filtered symbol's segment key out of step with the base table's global key for it.
    //
    // 'bbb' is in the base dictionary before the view is created, so an ordinary compile can resolve
    // it to a global key. The post-CREATE commits then land un-applied and back to back, so each one
    // restarts its local ids at the same stale cleanSymbolCount: 'ccc' and 'ddd' are handed the very
    // same segment key, and the segment key space no longer lines up with the base's. Whatever key a
    // filter baked in at compile time cannot be trusted against these segments.
    // Asserts CREATE LIVE VIEW refuses the given TWO_PASS window expression. These compile to a
    // CachedWindowRecordCursorFactory, so the factory-level reject fires - pin its multi-pass tail,
    // not just the shared prefix, so the two distinct reject messages stay distinguishable across
    // refactors. Assumes the caller already created the `base` table.
    private void assertTwoPassWindowFunctionRejected(String windowExpr) throws SqlException {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, " + windowExpr + " OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY val ORDER BY ts ANCHOR DAILY '00:00')");
            Assert.fail("expected SqlException for TWO_PASS window function " + windowExpr);
        } catch (SqlException e) {
            Assert.assertTrue(
                    windowExpr + ": " + e.getMessage(),
                    e.getMessage().contains(
                            "live view select may only use window functions that support incremental refresh; "
                                    + "this query requires caching or multi-pass evaluation")
            );
        }
    }

    private void assertSymbolEqualityFilterRefreshes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // Applied before the view exists, so 'aaa'/'bbb' are the base's clean dictionary.
            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('aaa', 1, '2026-01-01T00:00:00.000000Z'), " +
                    "('bbb', 2, '2026-01-01T00:01:00.000000Z')");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym = 'bbb'");

            // Two commits written before either is applied: both see cleanSymbolCount=2, so 'ccc' and
            // 'ddd' collide on the same local key, and every 'bbb' row here has to be matched through
            // the segment's own symbol space rather than a key resolved once against the base.
            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('ccc', 3, '2026-01-01T00:02:00.000000Z'), " +
                    "('bbb', 4, '2026-01-01T00:03:00.000000Z')");
            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('ddd', 5, '2026-01-01T00:04:00.000000Z'), " +
                    "('bbb', 6, '2026-01-01T00:05:00.000000Z'), " +
                    "('aaa', 7, '2026-01-01T00:06:00.000000Z'), " +
                    "(NULL, 8, '2026-01-01T00:07:00.000000Z')");
            drainWalQueue();

            // FLUSH EVERY 1s against the pinned clock means one batch per cycle; step the clock past
            // the rate limit until every batch has drained.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 1; i <= 4; i++) {
                    drainJob(job);
                    drainWalQueue();
                    setCurrentMicros(i * 2_000_000L);
                }
            }
            drainWalQueue();

            // rn advances only for survivors, so a leaked or dropped row perturbs it too. A filter
            // matching on a stale key would either drop the post-CREATE 'bbb' rows or admit the 'ccc'
            // / 'ddd' rows that collide with them in the segment's key space.
            //
            // val=2 is a pre-CREATE row, and it is in the view: the clock is pinned below the test
            // data, so START FROM NOW resolves to a boundary all these 2026 rows sit above, and the
            // initial seed feeds them through the same filter. Membership follows the row's
            // timestamp, not the commit that carried it.
            assertQuery("SELECT sym, val, rn FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\trn\n" +
                    "bbb\t2\t1\n" +
                    "bbb\t4\t2\n" +
                    "bbb\t6\t3\n");
            assertQuery("SELECT count() FROM lv WHERE sym <> 'bbb'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertLiveViewProgress(String label, long lastProcessedSeqTxn, long lvConsumedSeqTxn, long lagSeqTxn) throws Exception {
        try {
            assertQuery("SELECT view_status, last_processed_seqtxn, lv_consumed_seqtxn, lag_seqtxn FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tlast_processed_seqtxn\tlv_consumed_seqtxn\tlag_seqtxn\n"
                            + "active\t" + lastProcessedSeqTxn + '\t' + lvConsumedSeqTxn + '\t' + lagSeqTxn + '\n');
        } catch (AssertionError e) {
            throw new AssertionError(label + ": " + e.getMessage(), e);
        }
    }

    private void assertLiveViewWalReport(String label, String expectedRow) throws Exception {
        try {
            assertQuery("SELECT name, suspended, writerTxn, sequencerTxn, errorTag, errorMessage FROM wal_tables() WHERE name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("name\tsuspended\twriterTxn\tsequencerTxn\terrorTag\terrorMessage\n" + expectedRow);
        } catch (AssertionError e) {
            throw new AssertionError(label + ": " + e.getMessage(), e);
        }
    }

    // Every read context of the plan over the view and over the recompute of its SELECT from fromSql: the
    // rows, count(), LIMIT -1, a filter, a descending order, a CTE, a join with the base, UNION ALL and a
    // GROUP BY. A paused view must answer each as its SELECT does over the rows committed before the pause.
    private void assertPauseReads(String label, String fromSql) throws Exception {
        final String recompute = "(" + pauseViewSelect(fromSql) + ")";
        final String[] reads = {
                "SELECT * FROM %s",
                "SELECT count() FROM %s",
                "SELECT * FROM %s LIMIT -1",
                "SELECT * FROM %s WHERE x > 1",
                "SELECT * FROM %s ORDER BY ts DESC",
                "WITH c AS (SELECT * FROM %s) SELECT ts, x, rn FROM c",
                "SELECT v.ts, v.x, v.rn, b.wave FROM %s v JOIN base b ON v.ts = b.ts",
                "SELECT ts, x, rn FROM %1$s UNION ALL SELECT ts, x, rn FROM %1$s",
                "SELECT x, count() FROM %s GROUP BY x ORDER BY x"
        };
        for (String read : reads) {
            try {
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, String.format(read, recompute), String.format(read, "lv"), LOG);
            } catch (AssertionError e) {
                throw new AssertionError(label + " [read=" + read + "]: " + e.getMessage(), e);
            }
        }
    }

    // SUSPEND WAL with withClause, two base commits, passes past FLUSH EVERY and the retry budget, then
    // RESUME: the view serves the pre-suspension rows throughout the pause, its WAL does not move, and
    // wal_tables() reports the operator's suspension, until RESUME, after which it holds every row.
    private void assertSuspendWalPausesUntilResume(
            boolean isWriteDenied,
            String timestampType,
            int shape,
            boolean isEmpty,
            String withClause,
            String tag,
            String message
    ) throws Exception {
        final String label = "[writeDenied=" + isWriteDenied + ", ts=" + timestampType + ", shape=" + shape
                + ", empty=" + isEmpty + ", with=" + withClause + ']';
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, String.valueOf(isWriteDenied));
        setCurrentMicros(0);
        createPauseView(shape, timestampType);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            if (!isEmpty) {
                insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
            }
            drainWalQueue();
            driveRefreshToQuiescence(job);
            final LiveViewInstance instance = lvInstance();
            final SeqTxnTracker tracker = lvTracker();
            final long writerTxn = tracker.getWriterTxn();
            Assert.assertEquals(label, writerTxn, tracker.getSeqTxn());
            final long lastProcessed = instance.getLastProcessedSeqTxn();
            final long lvConsumed = instance.getStateReader().getLvConsumedSeqTxn();
            execute("ALTER LIVE VIEW lv SUSPEND WAL" + withClause);

            insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 1);
            drivePausePass(job);
            insertPauseRow("2026-04-01T02:00:00.000000Z", 3, 1);
            // The pass that dequeues the commit's notification reports it; nothing after reports work.
            drivePausePass(job);
            Assert.assertFalse(label + ": a paused view must report no work", drivePausePass(job));
            Assert.assertFalse(label + ": a paused view must report no work", drivePausePass(job));

            assertPauseReads(label, PAUSED_ROWS);
            assertLiveViewWalReport(label, "lv\ttrue\t" + writerTxn + '\t' + writerTxn + '\t' + tag + '\t' + message + '\n');
            assertLiveViewProgress(label, lastProcessed, lvConsumed, 2);
            Assert.assertEquals(label, 0, instance.getRefreshFaultCount());
            // The view's WAL does not grow while it is paused, so a wait for its applied head ends at once.
            assertQuery("SELECT wait_wal_table('lv')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("wait_wal_table\ntrue\n");

            execute("ALTER LIVE VIEW lv RESUME WAL");
            driveRefreshToQuiescence(job);
            assertPauseReads(label, "base");
            final long seqTxn = tracker.getSeqTxn();
            Assert.assertTrue(label + ": the view must have flushed the commits it owed", seqTxn > writerTxn);
            assertLiveViewWalReport(label, "lv\tfalse\t" + seqTxn + '\t' + seqTxn + "\t\t\n");
            assertNoRefreshFaults("lv");
        }
        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Creates base (ts, x, wave) and the pause fixture's view over it. Rows tagged wave = 0 are the ones
    // committed before a pause, so PAUSED_ROWS selects what a paused view must keep serving.
    private void createPauseView(int shape, String timestampType) throws SqlException {
        execute("CREATE TABLE base (ts " + timestampType + ", x INT, wave INT) TIMESTAMP(ts) PARTITION BY DAY WAL"
                + (shape == PAUSE_SHAPE_DEDUP ? " DEDUP UPSERT KEYS(ts)" : ""));
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + pauseViewSelect("base"));
    }

    // base (ts, x, wave) with 500 rows over 2026-04-01..04 and a START FROM BEGINNING view over it, whose sweep
    // seals and yields every cairo.live.view.checkpoint.rows = 50 rows, driven until one seed turn committed.
    private LiveViewInstance createSeedingView(LiveViewRefreshJob job) throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT, wave INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base SELECT timestamp_sequence('2026-04-01', 600_000_000L), x::INT, 0 FROM long_sequence(500)");
        drainWalQueue();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS " + pauseViewSelect("base"));
        final LiveViewInstance instance = lvInstance();
        for (int i = 0; i < 100 && instance.getSeedDataOffset() < 50; i++) {
            job.run();
            drainWalQueue();
        }
        Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
        Assert.assertTrue("a seed turn must have committed its rows", lvTracker().getSeqTxn() > 0);
        return instance;
    }

    // driveRefreshToQuiescence with the clock moved past FLUSH EVERY first, until lv has consumed every commit
    // the base applied. FLUSH EVERY gates both the lead's flush and the coupled cycle of a view over a
    // deduplicating base, which driveRefreshToQuiescence alone reads as quiescence.
    private void drivePastFlushEvery(LiveViewRefreshJob job) {
        final long baseHead = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base")).getWriterTxn();
        for (int i = 0; i < 16; i++) {
            setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
            driveRefreshToQuiescence(job);
            if (lvInstance().getLastProcessedSeqTxn() >= baseHead) {
                return;
            }
        }
        Assert.fail("live view 'lv' did not consume the base up to " + baseHead);
    }

    @Test
    public void testCreateLiveViewIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            // IF NOT EXISTS should succeed when the view already exists.
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewWithParenthesizedSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // A parenthesized SELECT must capture only the balanced inner query;
            // a stray leading '(' used to make the stored SQL fail to recompile.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS (SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            assertQuery("SELECT view_sql FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .noCircuitBreakerCheck() // catalogue function over the in-memory registry; no per-row checks
                    .returns("view_sql\nSELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateWritesLiveViewDefinitionToSequencerDir() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            final TableToken token = engine.verifyTableName("lv");
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path seqPath = new Path(); Path tablePath = new Path()) {
                // SequencerMetadata.create writes _lv into the sequencer dir so it
                // replicates with the sequencer metadata (the LV table dir is
                // rebuilt from WAL on a replica and never ships its own _lv).
                seqPath.of(configuration.getDbRoot()).concat(token).concat(WalUtils.SEQ_DIR)
                        .concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME);
                // createLiveView still writes _lv into the table dir as the
                // primary's own atomic CREATE commit marker.
                tablePath.of(configuration.getDbRoot()).concat(token)
                        .concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME);

                Assert.assertTrue("seq-dir _lv must exist for replication", ff.exists(seqPath.$()));
                Assert.assertTrue("table-dir _lv must still exist", ff.exists(tablePath.$()));

                // Both copies come from the same LiveViewDefinition.append, so the
                // replication copy is a byte-complete twin of the commit marker.
                final long seqLen = ff.length(seqPath.$());
                Assert.assertTrue("seq-dir _lv must be non-empty", seqLen > 0);
                Assert.assertEquals("seq-dir _lv must match table-dir _lv", ff.length(tablePath.$()), seqLen);
            }

            // The persisted definition round-trips: the base table name reads back.
            try (Path p = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
                p.of(configuration.getDbRoot());
                Assert.assertEquals("base", LiveViewDefinition.readBaseTableName(reader, p, p.size(), token));
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropAllTablesDropsLiveViewThroughLiveViewPath() throws Exception {
        // DROP ALL TABLES recognised the live view for authorization but then dropped it through
        // the generic dropTableOrViewOrMatView, which knows nothing about the LV-specific state.
        // Mat views get away with the generic call because their cleanup lives inside it; a live
        // view's lives in the separate CairoEngine.dropLiveView wrapper, so DROP ALL skipped the
        // registry removal, the dependents-graph edge, the durable _lv.drop sentinel and the
        // refresh-worker fence. The table went away but the view stayed in the registry - a zombie
        // that live_views() kept listing and that a re-CREATE under the same name would double-
        // register into the base's grow-only dependents list.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:01.000000Z', 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            Assert.assertNotNull(engine.getLiveViewRegistry().getViewInstance("lv"));

            execute("DROP ALL");
            drainWalQueue();

            Assert.assertNull(
                    "DROP ALL must deregister the live view, not leave a registry zombie",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
            assertQuery("SELECT count() FROM live_views()")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n0\n");
            assertQuery("SELECT count() FROM tables()")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n0\n");

            // Re-CREATE under the same name must land a single registration. Against a zombie the
            // base's dependents list would hold both instances, so the refresh worker, the
            // invalidation fan-out and the WAL purge floor would all walk a dead view.
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            final ObjList<LiveViewInstance> dependents = new ObjList<>();
            engine.getLiveViewRegistry().getViewsForBaseTable("base", dependents);
            Assert.assertEquals(
                    "re-CREATE after DROP ALL must not double-register [views=" + dependents.size() + ']',
                    1,
                    dependents.size()
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:02.000000Z', 7)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:02.000000Z\t7\t1\n");

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testDropPrunesCheckpointPurgeState() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) n FROM base");
            final TableToken token = engine.verifyTableName("lv");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            final long lifecycleIdentity = instance.getLifecycleIdentity();
            final LiveViewCheckpointPurgeState state = engine.getLiveViewCheckpointPurgeState();
            final LongList retired = new LongList();
            retired.add(11);
            state.markReconciled(lifecycleIdentity);
            state.replacePendingRetirements(lifecycleIdentity, retired);
            Assert.assertEquals(1, state.getActiveGenerationCountForTest());

            execute("DROP LIVE VIEW lv");

            Assert.assertEquals("real DROP must prune purge state", 0, state.getActiveGenerationCountForTest());
            Assert.assertEquals("real DROP must pool pending retirement shells", 1,
                    state.getRetirementPoolSizeForTest());

            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) n FROM base");
            final TableToken recreatedToken = engine.verifyTableName("lv");
            final LiveViewInstance recreated = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotEquals("recreated view must have a fresh lifecycle identity",
                    lifecycleIdentity, recreated.getLifecycleIdentity());
            Assert.assertNotEquals("recreated view must have a fresh table identity",
                    token.getTableId(), recreatedToken.getTableId());
            Assert.assertFalse("recreated path must not inherit reconciliation",
                    state.isReconciled(recreated.getLifecycleIdentity()));
            Assert.assertEquals("recreated path owns only its fresh generation", 1,
                    state.getActiveGenerationCountForTest());
        });
    }

    @Test
    public void testDropLiveViewIfExists() throws Exception {
        assertMemoryLeak(() -> {
            // No view exists yet — IF EXISTS must swallow the error.
            execute("DROP LIVE VIEW IF EXISTS nonexistent");
        });
    }

    @Test
    public void testDropLiveViewPurgesTableFiles() throws Exception {
        // A live view is a WAL table, so its on-disk directory is reclaimed by the standard
        // WAL machinery: the sequencer mints a DROP_TABLE_WAL_ID notification, ApplyWal2TableJob
        // sees the token is dropped and calls purgeTableFiles, and WalPurgeJob then removes the
        // token from tables.d. But ApplyWal2TableJob.doRun drops EVERY notification for a live
        // view on a primary (the LV refresh worker owns the writer there), including the drop -
        // so purgeTableFiles was never reached. WalPurgeJob then saw "dropped but files exist",
        // logged "pinging WAL Apply job to delete table files" and re-notified, forever: the
        // view's entire directory and its tables.d entry leaked across restarts, on the default
        // configuration.
        //
        // The 441 other DROP LIVE VIEW tests miss this because not one of them asserts the
        // directory or the token is actually gone.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, sum(x) OVER (PARTITION BY x ORDER BY ts " +
                    "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);
            // The refresh worker must be enabled for the primary-path early-return to engage;
            // that is the default, and it is what the state store reports here.
            Assert.assertTrue(engine.getLiveViewStateStore().isRefreshEnabled());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            execute("DROP LIVE VIEW lv");
            // Drive the apply job and the purge job to convergence. Reclaiming a dropped WAL
            // table takes both, in turn: ApplyWal2TableJob removes the table's files and
            // WalPurgeJob then rmdir's the shell and deregisters the token. Pre-fix this loop
            // never converges - the purge job re-pings the apply job on every pass, and the
            // apply job drops the notification on the floor again.
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                for (int i = 0; i < 8; i++) {
                    drainWalQueue();
                    purgeJob.drain(0);
                }
            }

            Assert.assertNull(
                    "the dropped live view's token must be gone from the name registry",
                    engine.getTableTokenIfExists("lv")
            );
            // TABLE_EXISTS means the _txn file is still there, i.e. purgeTableFiles never ran and
            // the view's partition data is still on disk. A dropped plain WAL table reaches
            // TABLE_RESERVED here (its txn_seq/ and wal1/ shells outlive this loop and are
            // reclaimed by the sequencer-release path), so that - not TABLE_DOES_NOT_EXIST - is
            // the state a correctly-purged live view must reach too. Pre-fix the view sat at
            // TABLE_EXISTS no matter how many times the two jobs ran.
            final Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot());
            Assert.assertNotEquals(
                    "the dropped live view's table files must be purged, as they are for a plain WAL table",
                    TableUtils.TABLE_EXISTS,
                    TableUtils.exists(
                            engine.getConfiguration().getFilesFacade(),
                            path,
                            engine.getConfiguration().getDbRoot(),
                            lvToken.getDirName()
                    )
            );
        });
    }

    @Test
    public void testDropNonExistentLiveViewFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("DROP LIVE VIEW nonexistent");
                Assert.fail("expected SqlException for missing live view");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view does not exist")
                );
            }
        });
    }

    @Test
    public void testHighIndexSymbolColumnWithNoSegmentDiffRefreshes() throws Exception {
        // WalReader.symbolMaps is only as long as the symbol columns a segment actually carries a
        // diff for, and getSymbolKey/Count/Value index it with the base writer index - guarded by
        // col < symbolMaps.size(). This pins the shape those guards exist for: a wide base whose
        // high-index SYMBOL contributes no diff at all (its rows are NULL) while a low-index one
        // does, so the list is genuinely shorter than the high column's index. The refresh must
        // complete and the column read back NULL.
        //
        // Note: removing the guards does NOT crash here. A column with no diff can only hold NULLs,
        // and a NULL never resolves through getSymbolValue/getSymbolCount, so the out-of-bounds read
        // is not reachable from the live-view path - the guards are defensive.
        assertMemoryLeak(() -> {
            // lo_sym (writer index 1) carries values, so it gets a diff and sizes symbolMaps to 2.
            // hi_sym (writer index 18) is always NULL, so it contributes no diff at all - its index
            // sits far past the end of that list.
            final StringBuilder cols = new StringBuilder("ts TIMESTAMP, lo_sym SYMBOL");
            for (int i = 1; i <= 16; i++) {
                cols.append(", c").append(i).append(" INT");
            }
            cols.append(", hi_sym SYMBOL");
            execute("CREATE TABLE base (" + cols + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, lo_sym, hi_sym, c1, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base " +
                    "WHERE hi_sym IS NULL");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, lo_sym, c1) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'aa', 10), " +
                        "('2026-01-01T00:00:02.000000Z', 'bb', 20)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, lo_sym, hi_sym, c1, rn FROM lv ORDER BY ts")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tlo_sym\thi_sym\tc1\trn\n" +
                            "2026-01-01T00:00:01.000000Z\taa\t\t10\t1\n" +
                            "2026-01-01T00:00:02.000000Z\tbb\t\t20\t2\n");
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testQueryOverAViewWhoseTableGrowsItsSymbolCapacityPlansAgainstThePooledReader() throws Exception {
        // The view's table starts at cairo.default.symbol.capacity whatever the base declares, so
        // the WAL apply that takes it past the auto-scale threshold rewrites its SYMBOL column.
        // TableWriter.changeSymbolCapacity swaps _meta first, then copies and rebuilds the symbol
        // files, and only then bumps the metadata version in _txn, so a pooled reader stays one
        // version behind _meta until the rebuild ends. A query compiled in that window must plan
        // against the pooled reader it runs on: planning against _meta read from disk failed every
        // recompile with "too many cached query plan cannot be used because table schema has changed".
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_CAPACITY, 16);
        final String[] viewDir = new String[1];
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger midRewriteQueries = new AtomicInteger();
        final AtomicReference<Throwable> queryError = new AtomicReference<>();
        final StringSink midRewriteResult = new StringSink();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int copy(LPSZ from, LPSZ to) {
                // The writer copies the symbol offset file after the _meta swap and before the bump.
                if (viewDir[0] != null
                        && Utf8s.containsAscii(from, viewDir[0])
                        && Utf8s.containsAscii(from, "sym.o")
                        && isArmed.compareAndSet(true, false)) {
                    midRewriteQueries.incrementAndGet();
                    final Thread reader = new Thread(() -> {
                        try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                            TestUtils.printSql(engine, ctx, "SELECT count(), count_distinct(sym) FROM lv", midRewriteResult);
                        } catch (Throwable th) {
                            queryError.set(th);
                        } finally {
                            clearWorkerThreadLocals();
                        }
                    });
                    reader.start();
                    try {
                        reader.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.copy(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL CAPACITY 1024, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, v, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            viewDir[0] = engine.verifyTableName("lv").getDirName();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Eight values stay below the 0.8 auto-scale threshold of a 16-value capacity.
                execute(insertDistinctSymbols(0, 8));
                drainWalQueue();
                driveRefreshToQuiescence(job);
                drainWalQueue();
                // Leaves an idle reader at the current metadata version in the pool.
                assertQuery("SELECT count(), count_distinct(sym) FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count\tcount_distinct
                                8\t8
                                """);

                // Twenty values cross the threshold: applying the view's commit grows its capacity.
                isArmed.set(true);
                execute(insertDistinctSymbols(8, 20));
                drainWalQueue();
                driveRefreshToQuiescence(job);
                drainWalQueue();
            }

            Assert.assertEquals("the query must run while the view's table rewrites its symbol capacity", 1, midRewriteQueries.get());
            final Throwable th = queryError.get();
            if (th != null) {
                throw new AssertionError("a query over the view failed while its table grew the SYMBOL column", th);
            }
            TestUtils.assertEquals("""
                    count\tcount_distinct
                    20\t20
                    """, midRewriteResult);
            assertQuery("SELECT count(), count_distinct(sym) FROM lv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tcount_distinct
                            20\t20
                            """);
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testSuspendedLiveViewCanBeResumed() throws Exception {
        // A live view is a WAL table, so a failing inline apply suspends it exactly like any
        // other (ApplyWal2TableJob.applyWal ends its catch in suspendTable), and
        // hasPendingLiveViewApply then skips it until an operator RESUMEs - the refresh job's own
        // comment says "only an operator RESUME clears it". But that recovery was unreachable:
        // compileAlterTable rejects every ALTER on a non-TABLE token up front, so
        // ALTER TABLE <lv> RESUME WAL died on "cannot modify live view", and no ALTER LIVE VIEW
        // grammar existed. A transient disk error during apply froze the view at its last applied
        // seqTxn forever, silently serving a stale prefix, with DROP + recreate the only escape.
        final String[] lvDir = new String[1];
        final AtomicBoolean failApply = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // Fail the apply's create of the day-2 LV partition, once, after the commit made
                // the block durable. The fault self-clears so the resumed apply reads cleanly.
                if (failApply.get()
                        && lvDir[0] != null
                        && Utf8s.endsWithAscii(name, "x.d")
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.containsAscii(name, "2026-04-02")
                        && failApply.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Baseline: row 1 flushes and applies cleanly.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // The failing flush suspends the LV: its block is committed but unapplied.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-02T00:00:00.000000Z', 2)");
                drainWalQueue();
                failApply.set(true);
                drainJob(job);
                Assert.assertTrue("the failed inline apply must suspend the live view",
                        engine.getTableSequencerAPI().isSuspended(lvToken));

                // The view is now STUCK. Its refresh worker cannot clear the suspension on its
                // own - hasPendingLiveViewApply skips suspended tables - so with no further base
                // traffic the committed-but-unapplied block never lands. Row 2 stays invisible
                // and the view serves a stale prefix indefinitely. (A *new* base commit would
                // self-heal it: the next flush's inline applyWalDirect bypasses the suspension
                // check. That makes this the idle / low-traffic view's failure mode - precisely
                // the case an operator cannot wait out.)
                for (int i = 0; i < 5; i++) {
                    setCurrentMicros(currentMicros + 2_000_000L);
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\n1\n");
                Assert.assertTrue("idle refresh cycles cannot clear the suspension",
                        engine.getTableSequencerAPI().isSuspended(lvToken));

                // The operator recovery. ALTER TABLE stays (correctly) refused for a live view -
                // its schema is a function of its SELECT - so the WAL-control verbs get their own
                // grammar, exactly as materialized views do.
                try {
                    execute("ALTER TABLE lv RESUME WAL");
                    Assert.fail("ALTER TABLE must not modify a live view");
                } catch (SqlException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("cannot modify live view"));
                }
                execute("ALTER LIVE VIEW lv RESUME WAL");
                Assert.assertFalse("RESUME WAL must clear the suspension",
                        engine.getTableSequencerAPI().isSuspended(lvToken));

                // It catches up with NO new base commit: the deferred block lands exactly once.
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-04-01T00:00:00.000000Z\t1\t1\n" +
                            "2026-04-02T00:00:00.000000Z\t2\t2\n");
        });
    }

    @Test
    public void testAlterLiveViewResumeWalFromTxn() throws Exception {
        // RESUME WAL FROM TRANSACTION|TXN <n>: the live-view grammar branch that
        // forwards an explicit resume-from seqTxn to the shared alterTableResume path.
        // Both keyword spellings parse, and a valid in-range txn clears the suspension.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            // Resume from the already-applied txn: always in range (<= next available)
            // and a no-op forward skip, so it exercises the FROM parse without rewinding.
            final long appliedTxn = engine.getTableSequencerAPI().getTxnTracker(lvToken).getWriterTxn();

            execute("ALTER LIVE VIEW lv SUSPEND WAL");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(lvToken));
            execute("ALTER LIVE VIEW lv RESUME WAL FROM TXN " + appliedTxn);
            Assert.assertFalse("RESUME WAL FROM TXN must clear the suspension",
                    engine.getTableSequencerAPI().isSuspended(lvToken));

            execute("ALTER LIVE VIEW lv SUSPEND WAL");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(lvToken));
            execute("ALTER LIVE VIEW lv RESUME WAL FROM TRANSACTION " + appliedTxn);
            Assert.assertFalse("RESUME WAL FROM TRANSACTION must clear the suspension",
                    engine.getTableSequencerAPI().isSuspended(lvToken));

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testAlterLiveViewSuspendResumeWal() throws Exception {
        // ALTER LIVE VIEW <name> SUSPEND WAL / RESUME WAL - the operator-initiated
        // WAL-control verbs. SUSPEND must flip the LV's sequencer to suspended AND
        // register the hard-suspend (so the apply job skips it); RESUME must clear both.
        // testSuspendedLiveViewCanBeResumed covers RESUME after a fault-induced suspend;
        // this covers the SUSPEND verb itself and its wal_tables() visibility.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
            final TableToken lvToken = engine.verifyTableName("lv");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));
            Assert.assertFalse(engine.isWalApplySuspended(lvToken));

            execute("ALTER LIVE VIEW lv SUSPEND WAL");
            Assert.assertTrue("SUSPEND WAL must suspend the live view",
                    engine.getTableSequencerAPI().isSuspended(lvToken));
            Assert.assertTrue("SUSPEND WAL must register the hard-suspend",
                    engine.isWalApplySuspended(lvToken));
            assertQuery("SELECT name, suspended FROM wal_tables() WHERE name = 'lv'")
                    .noLeakCheck().noRandomAccess().returns("name\tsuspended\nlv\ttrue\n");

            execute("ALTER LIVE VIEW lv RESUME WAL");
            Assert.assertFalse("RESUME WAL must clear the suspension",
                    engine.getTableSequencerAPI().isSuspended(lvToken));
            Assert.assertFalse("RESUME WAL must clear the hard-suspend",
                    engine.isWalApplySuspended(lvToken));
            assertQuery("SELECT name, suspended FROM wal_tables() WHERE name = 'lv'")
                    .noLeakCheck().noRandomAccess().returns("name\tsuspended\nlv\tfalse\n");

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testAlterLiveViewSuspendWalWithErrorTagAndMessage() throws Exception {
        // SUSPEND WAL WITH <tag>, <message> records an operator-supplied error tag and
        // message on the LV's sequencer, surfaced through wal_tables(). Covers the WITH
        // clause parse (tag resolved by name, plus message) and its round-trip.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
            final TableToken lvToken = engine.verifyTableName("lv");

            execute("ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'manual halt'");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(lvToken));
            assertQuery("SELECT suspended, errorTag, errorMessage FROM wal_tables() WHERE name = 'lv'")
                    .noLeakCheck().noRandomAccess()
                    .returns("suspended\terrorTag\terrorMessage\ntrue\tDISK FULL\tmanual halt\n");

            execute("ALTER LIVE VIEW lv RESUME WAL");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testAlterLiveViewWalControlRejectsBadGrammar() throws Exception {
        // Parse-error branches of the live-view WAL-control grammar. Each must reject
        // before any state change, so the view stays un-suspended throughout.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
            final TableToken lvToken = engine.verifyTableName("lv");

            assertAlterLiveViewRejected("ALTER LIVE VIEW lv FREEZE WAL", "'resume' or 'suspend' expected");
            assertAlterLiveViewRejected("ALTER LIVE VIEW lv RESUME", "'wal' expected");
            assertAlterLiveViewRejected("ALTER LIVE VIEW lv RESUME WAL bogus", "'from' expected");
            assertAlterLiveViewRejected("ALTER LIVE VIEW lv RESUME WAL FROM", "'transaction' or 'txn' expected");
            assertAlterLiveViewRejected("ALTER LIVE VIEW lv RESUME WAL FROM TXN abc", "invalid value");
            assertAlterLiveViewRejected("ALTER LIVE VIEW lv SUSPEND WAL WITH 'NOT A TAG', 'msg'", "invalid value");
            assertAlterLiveViewRejected("ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL'", "',' expected");

            Assert.assertFalse("no rejected WAL-control statement may change suspension state",
                    engine.getTableSequencerAPI().isSuspended(lvToken));

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testAlterLiveViewSuspendWalPausesUntilResume() throws Exception {
        // ALTER LIVE VIEW ... SUSPEND WAL pauses the whole view, in both write-denial modes: no refresh turn,
        // no inline apply, reads frozen on the rows committed before the pause, the view's WAL not growing,
        // and wal_tables() reporting the operator's suspension until RESUME WAL, which continues the view
        // from where it stopped. Covers the lead path and the applied-base drain of a deduplicating base,
        // over micro and nano designated timestamps, and every WITH clause form.
        final String[] withClauses = {" WITH 'DISK FULL', 'manual halt'", " WITH 999, 'диск полон ✓ 𝄞'", ""};
        final String[] tags = {"DISK FULL", "", ""};
        final String[] messages = {"manual halt", "диск полон ✓ 𝄞", ""};
        assertMemoryLeak(() -> {
            int row = 0;
            for (int denied = 0; denied < 2; denied++) {
                for (String timestampType : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                    for (int shape = PAUSE_SHAPE_LEAD; shape <= PAUSE_SHAPE_DEDUP; shape++) {
                        final int with = row++ % withClauses.length;
                        assertSuspendWalPausesUntilResume(denied == 1, timestampType, shape, false, withClauses[with], tags[with], messages[with]);
                    }
                }
                // SUSPEND before the view holds any row.
                assertSuspendWalPausesUntilResume(denied == 1, "TIMESTAMP", PAUSE_SHAPE_LEAD, true, withClauses[0], tags[0], messages[0]);
            }
        });
    }

    @Test
    public void testApplyRetryOfSuspendedLiveViewWaits() throws Exception {
        // A forward block the inline apply could not land - the view's writer was held - waits for the apply
        // retry. A view listed in cairo.wal.apply.suspended.tables keeps its suspended state running, so the
        // retry itself has to see the listing: it must not apply the block while the view is paused.
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                final TableToken lvToken = engine.verifyTableName("lv");
                final SeqTxnTracker tracker = lvTracker();
                insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 0);
                drainWalQueue();
                setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
                try (TableWriter ignored = engine.getWriter(lvToken, "held by the test")) {
                    job.run();
                }
                final long appliedTxn = tracker.getWriterTxn();
                Assert.assertTrue("the held writer must leave the block unapplied", appliedTxn < tracker.getSeqTxn());
                // An idle save point request leaves the block to the apply retry too.
                tracker.requestIdleEpoch(currentMicros / 1000L);
                job.takeIdleSavePointForTest(lvInstance());
                Assert.assertEquals(appliedTxn, tracker.getWriterTxn());
                Assert.assertTrue(tracker.isIdleEpochRequested());

                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, lvToken.getDirName());
                capture.drain();
                final int retriesBefore = capture.countOccurrences(APPLY_RETRY_LINE);
                Assert.assertFalse("the retry must not run on a paused view", job.retryPendingLiveViewApplyForTest(lvInstance()));
                Assert.assertEquals(appliedTxn, tracker.getWriterTxn());
                capture.drain();
                Assert.assertEquals(retriesBefore, capture.countOccurrences(APPLY_RETRY_LINE));

                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(tracker.getSeqTxn(), tracker.getWriterTxn());
                assertPauseReads("unlisted", "base");
            } finally {
                capture.stop();
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testConfigListedLiveViewPausesUntilUnlisted() throws Exception {
        // A live view listed in cairo.wal.apply.suspended.tables pauses as one an operator suspended does, in
        // both write-denial modes. The list never sets the suspended state, so wal_tables() reports the view
        // running, as it reports a listed table, and its WAL does not grow. RESUME WAL does not lift a
        // listing; unlisting does, and the view continues from where it stopped.
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            for (int denied = 0; denied < 2; denied++) {
                final String label = "[writeDenied=" + (denied == 1) + ']';
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, String.valueOf(denied == 1));
                setCurrentMicros(0);
                createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
                capture.start();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    final LiveViewInstance instance = lvInstance();
                    final SeqTxnTracker tracker = lvTracker();
                    final long writerTxn = tracker.getWriterTxn();
                    final long lastProcessed = instance.getLastProcessedSeqTxn();
                    final long lvConsumed = instance.getStateReader().getLvConsumedSeqTxn();
                    setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, engine.verifyTableName("lv").getDirName());

                    insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 1);
                    drivePausePass(job);
                    insertPauseRow("2026-04-01T02:00:00.000000Z", 3, 1);
                    drivePausePass(job);
                    Assert.assertFalse(label, drivePausePass(job));
                    assertPauseReads(label, PAUSED_ROWS);
                    assertLiveViewWalReport(label, "lv\tfalse\t" + writerTxn + '\t' + writerTxn + "\t\t\n");
                    assertLiveViewProgress(label, lastProcessed, lvConsumed, 2);

                    // RESUME WAL lifts a SUSPEND WAL, not a listing.
                    execute("ALTER LIVE VIEW lv RESUME WAL");
                    Assert.assertFalse(label, drivePausePass(job));
                    assertPauseReads(label, PAUSED_ROWS);
                    capture.drain();
                    capture.assertNotLogged(APPLY_RETRY_LINE);
                    Assert.assertEquals(label, 0, instance.getRefreshFaultCount());

                    setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
                    driveRefreshToQuiescence(job);
                    assertPauseReads(label, "base");
                    Assert.assertEquals(label, tracker.getSeqTxn(), tracker.getWriterTxn());
                    assertNoRefreshFaults("lv");
                } finally {
                    capture.stop();
                }
                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE base");
            }
        });
    }

    @Test
    public void testIdleRequestDoesNotSkipInlineApply() throws Exception {
        // A failed inline apply suspends the view, a fault rather than an operator's SUSPEND WAL, and the next
        // flush's inline apply lands the stranded block. An idle save point request pending at that flush must
        // not make the apply return early: that early return is for the queue-driven apply of a suspended table,
        // not for the refresh worker applying what it just committed. Otherwise reads miss the flushed rows until
        // another flush comes.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        final String[] lvDir = new String[1];
        final AtomicBoolean failApply = new AtomicBoolean(false);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failApply.get()
                        && lvDir[0] != null
                        && Utf8s.endsWithAscii(name, "x.d")
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.containsAscii(name, "2026-04-02")
                        && failApply.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                insertPauseRow("2026-04-02T00:00:00.000000Z", 2, 0);
                drainWalQueue();
                failApply.set(true);
                drainJob(job);
                final SeqTxnTracker tracker = lvTracker();
                Assert.assertTrue("the failed inline apply must suspend the view", tracker.isSuspended());
                Assert.assertFalse("a fault suspension is not an operator's", engine.isWalApplySuspended(lvToken));
                Assert.assertTrue(tracker.getWriterTxn() < tracker.getSeqTxn());

                tracker.requestIdleEpoch(currentMicros / 1000L);
                final boolean isSlotA = tracker.isPinnedEpochSlotA();
                setCurrentMicros(4_000_000L);
                insertPauseRow("2026-04-02T01:00:00.000000Z", 3, 0);
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("the flush's inline apply must land every committed block", tracker.getSeqTxn(), tracker.getWriterTxn());
                Assert.assertFalse(tracker.isSuspended());
                Assert.assertFalse("the apply consumes the request", tracker.isIdleEpochRequested());
                Assert.assertNotEquals("the apply takes one save point", isSlotA, tracker.isPinnedEpochSlotA());
                assertQuery("SELECT ts, x, rn FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                2026-04-01T00:00:00.000000Z\t1\t1
                                2026-04-02T00:00:00.000000Z\t2\t2
                                2026-04-02T01:00:00.000000Z\t3\t3
                                """);
                assertPauseReads("after the flush", "base");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInFlightTurnWhenSuspendLandsKeepsTheSuspension() throws Exception {
        // SUSPEND WAL lands after a refresh turn passed every gate ahead of its latch. With write denial off
        // that turn finishes as an apply in flight does - its block commits and lands - and the apply's
        // progress must leave the operator's suspension standing. From then on the view is paused until
        // RESUME. With an idle save point request pending as well, the turn's inline apply still lands.
        assertMemoryLeak(() -> {
            for (int idle = 0; idle < 2; idle++) {
                final boolean isIdleRequested = idle == 1;
                final String label = "[idleRequest=" + isIdleRequested + ']';
                setCurrentMicros(0);
                createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    final SeqTxnTracker tracker = lvTracker();
                    Assert.assertEquals(label, 1, tracker.getWriterTxn());
                    hooks(job).setSimulatePeerTurnBeforeRefreshLatch(() -> {
                        if (isIdleRequested) {
                            tracker.requestIdleEpoch(currentMicros / 1000L);
                        }
                        suspendLiveViewWal(" WITH 'DISK FULL', 'manual halt'");
                    });
                    setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
                    insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 0);
                    drainWalQueue();
                    job.run();
                    Assert.assertTrue(label + ": the hook must have run", engine.isWalApplySuspended(engine.verifyTableName("lv")));
                    assertLiveViewWalReport(label, "lv\ttrue\t2\t2\tDISK FULL\tmanual halt\n");
                    assertPauseReads(label, "base");

                    insertPauseRow("2026-04-01T02:00:00.000000Z", 3, 1);
                    drivePausePass(job);
                    insertPauseRow("2026-04-01T03:00:00.000000Z", 4, 1);
                    drivePausePass(job);
                    Assert.assertFalse(label, drivePausePass(job));
                    assertLiveViewWalReport(label, "lv\ttrue\t2\t2\tDISK FULL\tmanual halt\n");
                    assertPauseReads(label, PAUSED_ROWS);

                    execute("ALTER LIVE VIEW lv RESUME WAL");
                    driveRefreshToQuiescence(job);
                    assertPauseReads(label, "base");
                    assertLiveViewWalReport(label, "lv\tfalse\t" + tracker.getSeqTxn() + '\t' + tracker.getSeqTxn() + "\t\t\n");
                    assertNoRefreshFaults("lv");
                }
                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE base");
            }
        });
    }

    @Test
    public void testPublishedLeadSurvivesThePause() throws Exception {
        // A drain published an un-flushed lead into the in-memory tier before SUSPEND WAL. The paused view
        // must keep serving it - the lead stays in RAM under its stamp, as under a memory-pressure back-off -
        // and must not flush it, however long the pause outlasts FLUSH EVERY. RESUME flushes it.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, wave INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 10s START FROM NOW AS " + pauseViewSelect("base"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = lvInstance();
                final SeqTxnTracker tracker = lvTracker();
                final long flushedSeqTxn = tracker.getSeqTxn();
                // FLUSH EVERY is not due, so the drain publishes the row as a lead.
                instance.setLastFlushTimeUs(currentMicros);
                insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 0);
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("the drain must publish a lead", 1, instance.getLeadRowCount());
                Assert.assertEquals(flushedSeqTxn, tracker.getSeqTxn());
                assertPauseReads("lead published", "base");

                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                insertPauseRow("2026-04-01T02:00:00.000000Z", 3, 1);
                for (int pass = 0; pass < 3; pass++) {
                    drivePausePass(job);
                }
                Assert.assertFalse(drivePausePass(job));
                Assert.assertEquals("a paused view must not flush its lead", flushedSeqTxn, tracker.getSeqTxn());
                Assert.assertEquals(1, instance.getLeadRowCount());
                assertPauseReads("paused", PAUSED_ROWS);

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(0, instance.getLeadRowCount());
                Assert.assertEquals(tracker.getSeqTxn(), tracker.getWriterTxn());
                assertPauseReads("resumed", "base");
                assertNoRefreshFaults("lv");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropLiveViewDuringSuspension() throws Exception {
        // DROP LIVE VIEW of a paused view, in every shape a pause can hold: a SEEDING view with its sweep's
        // base reader pinned, and a view whose forward block a held writer left unapplied before SUSPEND WAL.
        // The DROP succeeds, frees what the view held and unregisters it, and a view created again under the
        // same name holds the recompute.
        assertMemoryLeak(() -> {
            for (int shape = 0; shape < 2; shape++) {
                final String label = shape == 0 ? "[seeding]" : "[unapplied block]";
                setCurrentMicros(0);
                execute("CREATE TABLE base (ts TIMESTAMP, x INT, wave INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    if (shape == 0) {
                        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 50);
                        execute("INSERT INTO base SELECT timestamp_sequence('2026-04-01', 600_000_000L), x::INT, 0 FROM long_sequence(500)");
                        drainWalQueue();
                        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS " + pauseViewSelect("base"));
                        final LiveViewInstance instance = lvInstance();
                        for (int i = 0; i < 100 && instance.getSeedDataOffset() < 50; i++) {
                            job.run();
                            drainWalQueue();
                        }
                        Assert.assertEquals(label, LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
                    } else {
                        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + pauseViewSelect("base"));
                        insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                        drainWalQueue();
                        driveRefreshToQuiescence(job);
                        insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 0);
                        drainWalQueue();
                        setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
                        try (TableWriter ignored = engine.getWriter(engine.verifyTableName("lv"), "held by the test")) {
                            job.run();
                        }
                        Assert.assertTrue(label, lvTracker().getWriterTxn() < lvTracker().getSeqTxn());
                    }
                    final LiveViewInstance instance = lvInstance();
                    execute("ALTER LIVE VIEW lv SUSPEND WAL");
                    insertPauseRow("2026-04-10T00:00:00.000000Z", 7, 1);
                    drivePausePass(job);
                    Assert.assertFalse(label, drivePausePass(job));

                    execute("DROP LIVE VIEW lv");
                    Assert.assertTrue(label, instance.isDropped());
                    Assert.assertNull(label, engine.getLiveViewRegistry().getViewInstance("lv"));
                    assertQuery("SELECT count() FROM live_views()")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n0\n");
                    drivePausePass(job);

                    execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS " + pauseViewSelect("base"));
                    driveSeedToCompletion(job, "lv");
                    driveRefreshToQuiescence(job);
                    assertPauseReads(label, "base");
                    assertNoRefreshFaults("lv");
                }
                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE base");
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, null);
            }
        });
    }

    @Test
    public void testPausedLiveViewCatchesUpABacklogAcrossTurns() throws Exception {
        // 200 base commits over 10,000 distinct keys and three day partitions land during the pause. After
        // RESUME WAL the catch-up spans many turns of a small turn budget and page frames, the keyed window
        // maps grow, and the view ends on the recompute. Then a failing apply during a second catch-up
        // suspends the view as a fault, not a pause, which a further RESUME WAL clears.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_TURN_MAX_COMMITS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        final String[] lvDir = new String[1];
        final AtomicBoolean failApply = new AtomicBoolean(false);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failApply.get()
                        && lvDir[0] != null
                        && Utf8s.endsWithAscii(name, "x.d")
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.containsAscii(name, "2026-04-05")
                        && failApply.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        final String viewSelect = "SELECT ts, sym, x, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rn, "
                + "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sx FROM ";
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + viewSelect + "base");
            lvDir[0] = engine.verifyTableName("lv").getDirName();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-04-01T00:00:00.000000Z', 's0', 1)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = lvInstance();
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                // 200 commits of 50 rows, each row a new key, spread over 2026-04-01..03.
                final long firstTs = MicrosTimestampDriver.floor("2026-04-01T00:00:01.000000Z");
                for (int c = 0; c < 200; c++) {
                    execute("INSERT INTO base SELECT timestamp_sequence(" + (firstTs + c * 1_296_000_000L)
                            + "L, 25_920_000L), 's' || (" + (c * 50) + " + x), x::INT FROM long_sequence(50)");
                }
                drainWalQueue();
                // The passes that dequeue the commits' notifications report them; nothing after reports work.
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && drivePausePass(job); pass++) {
                }
                Assert.assertFalse(drivePausePass(job));
                final long baseHead = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base")).getWriterTxn();
                Assert.assertTrue(instance.getLastProcessedSeqTxn() < baseHead - 100);

                execute("ALTER LIVE VIEW lv RESUME WAL");
                setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
                job.run();
                Assert.assertTrue("one turn must not catch the whole backlog up", instance.getRefreshedUpToSeqTxn() < baseHead);
                drivePastFlushEvery(job);
                Assert.assertEquals(baseHead, instance.getLastProcessedSeqTxn());
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSelect + "base", "lv", LOG);
                assertQuery("SELECT count(), count_distinct(sym) FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\tcount_distinct\n10001\t10001\n");

                // A fault, not a pause: the apply of the view's next block fails once and suspends it.
                execute("INSERT INTO base VALUES ('2026-04-05T00:00:00.000000Z', 's0', 2)");
                drainWalQueue();
                failApply.set(true);
                setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
                drainJob(job);
                Assert.assertTrue(lvTracker().isSuspended());
                Assert.assertFalse(engine.isWalApplySuspended(engine.verifyTableName("lv")));
                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveRefreshToQuiescence(job);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSelect + "base", "lv", LOG);
                Assert.assertFalse(instance.isInvalid());
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPausedLiveViewSharedByTwoWorkers() throws Exception {
        // Two refresh workers split the fallback scan by table id and both serve the notification queue.
        // Neither drives a paused view, whichever of them owns it, and once RESUME WAL lands the owner
        // catches it up.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            try (
                    LiveViewRefreshJob job0 = new LiveViewRefreshJob(0, engine, 2);
                    LiveViewRefreshJob job1 = new LiveViewRefreshJob(1, engine, 2)
            ) {
                insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                drainWalQueue();
                driveRefreshToQuiescence(job0);
                driveRefreshToQuiescence(job1);
                assertPauseReads("before", "base");
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 1);
                drainWalQueue();
                drainJob(job0);
                drainJob(job1);
                insertPauseRow("2026-04-01T02:00:00.000000Z", 3, 1);
                drainWalQueue();
                drainJob(job1);
                drainJob(job0);
                for (int pass = 0; pass < 2; pass++) {
                    setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
                    Assert.assertFalse("worker 0 must not drive a paused view", drainJob(job0));
                    Assert.assertFalse("worker 1 must not drive a paused view", drainJob(job1));
                }
                assertPauseReads("paused", PAUSED_ROWS);

                execute("ALTER LIVE VIEW lv RESUME WAL");
                for (int pass = 0; pass < 4; pass++) {
                    driveRefreshToQuiescence(job0);
                    driveRefreshToQuiescence(job1);
                }
                assertPauseReads("resumed", "base");
                assertNoRefreshFaults("lv");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testResumeWalFromTxnOnPausedLiveViewSkipsNothing() throws Exception {
        // Nothing reaches the view's WAL during a pause, so RESUME WAL FROM TXN has nothing to skip at any
        // transaction it accepts, which ends at the next one; a later transaction is refused at the view's
        // name, and the view stays paused.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                final long appliedTxn = lvTracker().getWriterTxn();
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 1);
                insertPauseRow("2026-04-01T02:00:00.000000Z", 3, 1);
                drivePausePass(job);
                Assert.assertFalse(drivePausePass(job));

                assertExceptionNoLeakCheck(
                        "ALTER LIVE VIEW lv RESUME WAL FROM TXN " + (appliedTxn + 2),
                        16,
                        "resume txn is higher than next available transaction [resumeFromTxn=" + (appliedTxn + 2) + ", nextTxn=" + (appliedTxn + 1) + ']'
                );
                Assert.assertTrue(engine.isWalApplySuspended(engine.verifyTableName("lv")));
                Assert.assertFalse(drivePausePass(job));
                assertPauseReads("paused", PAUSED_ROWS);

                execute("ALTER LIVE VIEW lv RESUME WAL FROM TXN " + (appliedTxn + 1));
                driveRefreshToQuiescence(job);
                assertPauseReads("resumed", "base");
                assertNoRefreshFaults("lv");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRowsOfAPartitionDroppedDuringThePauseFollowTheShadowBase() throws Exception {
        // Rows committed to a new day during the pause, then that day's partition dropped, and a one-shot
        // mid-drain fault armed: after RESUME WAL the drain reads the rows from the base WAL, as a view that
        // lagged by the same commits does, and the fault's recovery restores rather than rebuilds. The view
        // keeps the rows (freeze-and-continue): it matches its SELECT over a shadow base that got every insert
        // and no removal, stays valid and is not stopped.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "x");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(fault.facade(), () -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            execute("CREATE TABLE shadow (ts TIMESTAMP, x INT, wave INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            fault.of(engine.verifyTableName("base").getDirName());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertWithShadow("('2026-04-01T00:00:00.000000Z', 1, 0), ('2026-04-01T01:00:00.000000Z', 2, 0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                insertWithShadow("('2026-04-02T00:00:00.000000Z', 3, 1)");
                insertWithShadow("('2026-04-02T01:00:00.000000Z', 4, 1)");
                execute("ALTER TABLE base DROP PARTITION LIST '2026-04-02'");
                drainWalQueue();
                fault.arm(1);
                drivePausePass(job);
                Assert.assertFalse(drivePausePass(job));

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveRefreshToQuiescence(job);
                Assert.assertTrue("the drain after RESUME must meet the armed fault", fault.hasFired());
                final LiveViewInstance instance = lvInstance();
                Assert.assertFalse(instance.isInvalid());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, pauseViewSelect("shadow"), "lv", LOG);
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSuspendedLiveViewBaseSchemaChangeAndMidDrainFault() throws Exception {
        // A base ADD COLUMN, rows with and without the new column, and a one-shot mid-drain fault all land
        // while the view is paused. No turn runs, so nothing meets them, until the first drain after RESUME WAL
        // meets the schema drift and the fault together, as a view that lagged by the same commits does: it
        // stays valid and ends on the recompute. Over the raw-WAL lead view and a deduplicating base, and again
        // with the base's oldest partition dropped during the pause, where the view keeps that day's rows
        // (freeze-and-continue), follows a shadow base that got every insert and no removal, and is not
        // stopped by a rebuild the lost rows would refuse.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "x");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(fault.facade(), () -> {
            for (int shape = PAUSE_SHAPE_LEAD; shape <= PAUSE_SHAPE_DEDUP; shape++) {
                for (int drop = 0; drop < 2; drop++) {
                    final String label = "[shape=" + shape + ", dropOldestPartition=" + (drop == 1) + ']';
                    setCurrentMicros(0);
                    createPauseView(shape, "TIMESTAMP");
                    execute("CREATE TABLE shadow (ts TIMESTAMP, x INT, wave INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    fault.of(engine.verifyTableName("base").getDirName());
                    try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                        insertWithShadow("('2026-04-01T00:00:00.000000Z', 1, 0)");
                        insertWithShadow("('2026-04-02T00:00:00.000000Z', 2, 0)");
                        drainWalQueue();
                        drivePastFlushEvery(job);
                        execute("ALTER LIVE VIEW lv SUSPEND WAL");
                        if (drop == 1) {
                            execute("ALTER TABLE base DROP PARTITION LIST '2026-04-01'");
                        }
                        execute("ALTER TABLE base ADD COLUMN note INT");
                        execute("ALTER TABLE shadow ADD COLUMN note INT");
                        execute("INSERT INTO base (ts, x, wave, note) VALUES ('2026-04-03T00:00:00.000000Z', 3, 1, 30)");
                        execute("INSERT INTO shadow (ts, x, wave, note) VALUES ('2026-04-03T00:00:00.000000Z', 3, 1, 30)");
                        insertWithShadow("('2026-04-03T01:00:00.000000Z', 4, 1)");
                        drainWalQueue();
                        // A deduplicating base whose WAL holds a data-shaped non-DATA commit - the DROP PARTITION -
                        // drains the applied base over it rather than the raw WAL.
                        final boolean isAppliedScan = shape == PAUSE_SHAPE_DEDUP && drop == 1;
                        if (isAppliedScan) {
                            fault.armAppliedScan("2026-04-03");
                        } else {
                            fault.arm(1);
                        }
                        drivePausePass(job);
                        Assert.assertFalse(label, drivePausePass(job));
                        final LiveViewInstance instance = lvInstance();
                        Assert.assertFalse(label, instance.isInvalid());
                        Assert.assertEquals(label + ": no turn ran, so nothing met the fault", 0, instance.getRefreshFaultCount());
                        TestUtils.assertSqlCursors(engine, sqlExecutionContext, pauseViewSelect("(SELECT * FROM shadow WHERE wave = 0)"), "lv", LOG);

                        execute("ALTER LIVE VIEW lv RESUME WAL");
                        drivePastFlushEvery(job);
                        Assert.assertTrue(label + ": the drain after RESUME must meet the armed fault",
                                isAppliedScan ? fault.hasAppliedScanFired() : fault.hasFired());
                        Assert.assertFalse(label, instance.isInvalid());
                        Assert.assertFalse(label, instance.isCheckpointRecoveryBlocked());
                        TestUtils.assertSqlCursors(engine, sqlExecutionContext, pauseViewSelect("shadow"), "lv", LOG);

                        insertWithShadow("('2026-04-03T02:00:00.000000Z', 5, 1)");
                        drainWalQueue();
                        drivePastFlushEvery(job);
                        TestUtils.assertSqlCursors(engine, sqlExecutionContext, pauseViewSelect("shadow"), "lv", LOG);
                        Assert.assertFalse(label, instance.isInvalid());
                    } finally {
                        fault.disarmAll();
                    }
                    execute("DROP LIVE VIEW lv");
                    execute("DROP TABLE base");
                    execute("DROP TABLE shadow");
                }
            }
        });
    }

    @Test
    public void testSuspendedLiveViewDoesNotTouchItsWriters() throws Exception {
        // A paused view acquires neither its TableWriter nor its WalWriter, whatever the base commits and
        // however many passes run, in both write-denial modes: no turn, no apply retry, no idle save point.
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            for (int denied = 0; denied < 2; denied++) {
                final String label = "[writeDenied=" + (denied == 1) + ']';
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, String.valueOf(denied == 1));
                setCurrentMicros(0);
                createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
                capture.start();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    execute("ALTER LIVE VIEW lv SUSPEND WAL");
                    final AtomicInteger tableWriterAcquisitions = new AtomicInteger();
                    final AtomicInteger walWriterAcquisitions = new AtomicInteger();
                    engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                        if (tableToken != null
                                && "lv".equals(tableToken.getTableName())
                                && (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE)) {
                            if (factoryType == PoolListener.SRC_WRITER) {
                                tableWriterAcquisitions.incrementAndGet();
                            } else if (factoryType == PoolListener.SRC_WAL_WRITER) {
                                walWriterAcquisitions.incrementAndGet();
                            }
                        }
                    });
                    try {
                        for (int c = 0; c < 3; c++) {
                            insertPauseRow("2026-04-01T0" + (c + 1) + ":00:00.000000Z", c + 2, 1);
                            drivePausePass(job);
                        }
                        Assert.assertFalse(label, drivePausePass(job));
                    } finally {
                        engine.setPoolListener(null);
                    }
                    Assert.assertEquals(label + ": TableWriter acquisitions of the paused view", 0, tableWriterAcquisitions.get());
                    Assert.assertEquals(label + ": WalWriter acquisitions of the paused view", 0, walWriterAcquisitions.get());
                    capture.drain();
                    capture.assertNotLogged("unsolicited table lock");
                    execute("ALTER LIVE VIEW lv RESUME WAL");
                    driveRefreshToQuiescence(job);
                    assertPauseReads(label, "base");
                } finally {
                    capture.stop();
                }
                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE base");
            }
        });
    }

    @Test
    public void testSuspendedLiveViewLateRowMatchesRecompute() throws Exception {
        // A late base row committed while the view is paused is first read by the drain after RESUME WAL,
        // over a view table that holds every committed block, so the out-of-order repair runs as for a view
        // that lagged by the same commits. Rows only, against the recompute: during the pause, after RESUME
        // and after a restart.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 0; i < 3; i++) {
                    insertPauseRow("2026-04-01T0" + i + ":00:00.000000Z", i + 1, 0);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                }
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                insertPauseRow("2026-04-01T03:00:00.000000Z", 4, 1);
                insertPauseRow("2026-04-01T01:30:00.000000Z", 5, 1);
                drivePausePass(job);
                Assert.assertFalse(drivePausePass(job));
                assertPauseReads("paused", PAUSED_ROWS);
                Assert.assertFalse(lvInstance().isInvalid());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveRefreshToQuiescence(job);
                assertPauseReads("resumed", "base");
            }
            restartLiveViewsAndRecover();
            assertPauseReads("restarted", "base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSuspendedLiveViewSymbolsAndVarSizeAcrossResume() throws Exception {
        // A paused view keeps serving SYMBOL, VARCHAR and DOUBLE[] output as it was: symbol values first
        // committed to the base during the pause, empty, NULL, multi-byte, 4-byte and 10 KB VARCHARs and
        // arrays all wait for RESUME WAL, after which the view holds them.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, s SYMBOL, v VARCHAR, arr DOUBLE[], wave INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String viewSelect = "SELECT ts, s, v, arr, count(*) OVER (PARTITION BY s ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM ";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + viewSelect + "base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2026-04-01T00:00:00.000000Z', 'a', 'plain', ARRAY[1.0, 2.0], 0), "
                        + "('2026-04-01T00:01:00.000000Z', NULL, NULL, NULL, 0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                execute("INSERT INTO base VALUES "
                        + "('2026-04-01T00:02:00.000000Z', 'new_during_pause', '', ARRAY[-0.0, 3.5], 1), "
                        + "('2026-04-01T00:03:00.000000Z', 'a', 'привет мир', ARRAY[0.5], 1), "
                        + "('2026-04-01T00:04:00.000000Z', 'другой', '𝄞 four bytes 😀', NULL, 1), "
                        + "('2026-04-01T00:05:00.000000Z', 'a', rpad('x', 10_240, 'y'), ARRAY[1.0, NULL], 1)");
                drainWalQueue();
                drivePausePass(job);
                Assert.assertFalse(drivePausePass(job));
                final String paused = "(SELECT * FROM base WHERE wave = 0)";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSelect + paused, "lv", LOG);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT count() FROM (" + viewSelect + paused + ")", "SELECT count() FROM lv", LOG);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT * FROM (" + viewSelect + paused + ") LIMIT -1", "SELECT * FROM lv LIMIT -1", LOG);

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveRefreshToQuiescence(job);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSelect + "base", "lv", LOG);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT * FROM (" + viewSelect + "base) LIMIT -1", "SELECT * FROM lv LIMIT -1", LOG);
                assertNoRefreshFaults("lv");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testConfigListedLiveViewHeldAcrossRestart() throws Exception {
        // A view listed in cairo.wal.apply.suspended.tables stays paused across a restart, in both write-denial
        // modes: the restart's recovery waits unattempted, the job reports no work, the view stays active with
        // its consumed watermark, and the WAL purge keeps the base WAL above it. Unlisting runs the restart's
        // recovery from the timeline the view sealed, and the view continues to the recompute.
        configureWalPurge();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            for (int denied = 0; denied < 2; denied++) {
                final String label = "[writeDenied=" + (denied == 1) + ']';
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, String.valueOf(denied == 1));
                setCurrentMicros(0);
                createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                    insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 0);
                    drainWalQueue();
                    drivePastFlushEvery(job);
                }
                final long lvConsumed = lvInstance().getStateReader().getLvConsumedSeqTxn();
                final long lastProcessed = lvInstance().getLastProcessedSeqTxn();
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, engine.verifyTableName("lv").getDirName());
                insertPauseRow("2026-04-01T02:00:00.000000Z", 3, 1);
                insertPauseRow("2026-04-01T03:00:00.000000Z", 4, 1);
                drainWalQueue();
                final long baseHead = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base")).getWriterTxn();

                engine.getLiveViewRegistry().clear();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseInactive();
                engine.buildViewGraphs();
                final LiveViewInstance instance = lvInstance();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    drivePausePass(job);
                    Assert.assertFalse(label, drivePausePass(job));
                    Assert.assertFalse(label + ": the restart's recovery waits for the unlisting", instance.isCheckpointRestoreAttempted());
                    assertLiveViewProgress(label, lastProcessed, lvConsumed, baseHead - lastProcessed);
                    Assert.assertTrue(
                            label + ": the WAL purge must keep every base segment above the view's consumed watermark",
                            purgeBaseWalAndCountSegments() >= baseHead - lvConsumed
                    );
                    assertPauseReads(label, PAUSED_ROWS);

                    setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
                    drivePastFlushEvery(job);
                    assertRestoredFromTimeline("lv");
                    assertPauseReads(label, "base");
                    assertNoRefreshFaults("lv");
                }
                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE base");
            }
        });
    }

    @Test
    public void testSuspendWalDoesNotSurviveAnEngineRestart() throws Exception {
        // SUSPEND WAL is not persisted, so a restart of the engine lifts it from a live view as it lifts it from
        // a table: the restarted engine reports the view running, restores it from its timeline and catches it
        // up with the base commits of the pause.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                drainWalQueue();
                drivePastFlushEvery(job);
                execute("ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'manual halt'");
                insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 1);
                drainWalQueue();
                drivePausePass(job);
                Assert.assertFalse(drivePausePass(job));
            }
            assertLiveViewWalReport("before the restart", "lv\ttrue\t1\t1\tDISK FULL\tmanual halt\n");
            // The process stops: nothing of this engine holds the view or its base any more.
            engine.getLiveViewRegistry().clear();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
            engine.releaseInactive();
            engine.releaseInactiveTableSequencers();
            final StringSink sink = new StringSink();
            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext context = new SqlExecutionContextImpl(restarted, 1).with(securityContext)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                TestUtils.printSql(restarted, context, "SELECT name, suspended, errorTag, errorMessage FROM wal_tables() WHERE name = 'lv'", sink);
                TestUtils.assertEquals("name\tsuspended\terrorTag\terrorMessage\nlv\tfalse\t\t\n", sink);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, restarted, 1)) {
                    boolean isWork = true;
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && isWork; pass++) {
                        setCurrentMicros(currentMicros + PAUSE_PASS_MICROS);
                        drainWalQueue(restarted);
                        isWork = drainJob(job);
                        drainWalQueue(restarted);
                    }
                    Assert.assertFalse("the restarted view must quiesce", isWork);
                    final LiveViewInstance instance = restarted.getLiveViewRegistry().getViewInstance("lv");
                    Assert.assertNotNull(instance);
                    assertRestoredFromTimeline("lv", instance);
                    Assert.assertEquals(0, instance.getRefreshFaultCount());
                }
                TestUtils.assertSqlCursors(restarted, context, pauseViewSelect("base"), "lv", LOG);
                TestUtils.printSql(restarted, context, "SELECT name, suspended, writerTxn, sequencerTxn FROM wal_tables() WHERE name = 'lv'", sink);
                TestUtils.assertEquals("name\tsuspended\twriterTxn\tsequencerTxn\nlv\tfalse\t2\t2\n", sink);
                restarted.getLiveViewRegistry().clear();
                restarted.releaseAllReaders();
                restarted.releaseAllWriters();
                restarted.releaseAllWalWriters();
                restarted.releaseInactive();
            }
        });
    }

    @Test
    public void testSuspendedSeedingViewConfigListedAcrossRestartWithCommittedBlock() throws Exception {
        // A seed turn committed a block its inline apply could not land (the view's writer was held), and then
        // the view was listed in cairo.wal.apply.suspended.tables and restarted. The listed view holds its
        // sweep: nothing applies the block, the job reports no work, the view stays SEEDING. Unlisting lets the
        // sweep's resume apply the block first and complete without a duplicate row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 50);
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                createSeedingView(job);
                try (TableWriter ignored = engine.getWriter(engine.verifyTableName("lv"), "held by the test")) {
                    job.run();
                }
            }
            final SeqTxnTracker tracker = lvTracker();
            final long appliedTxn = tracker.getWriterTxn();
            Assert.assertTrue("the held writer must leave the seed block unapplied", appliedTxn < tracker.getSeqTxn());
            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, engine.verifyTableName("lv").getDirName());
            engine.getLiveViewRegistry().clear();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseInactive();
            engine.buildViewGraphs();
            final LiveViewInstance instance = lvInstance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drivePausePass(job);
                Assert.assertFalse(drivePausePass(job));
                Assert.assertEquals(appliedTxn, tracker.getWriterTxn());
                Assert.assertTrue(tracker.getWriterTxn() < tracker.getSeqTxn());
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());

                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
                driveSeedToCompletion(job, "lv");
                drivePastFlushEvery(job);
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                Assert.assertEquals(tracker.getSeqTxn(), tracker.getWriterTxn());
                assertPauseReads("unlisted", "base");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSuspendedSeedingViewFaultInTheTurnSuspendLandsIn() throws Exception {
        // A seed turn faults, and SUSPEND WAL lands while that turn handles its fault. The fault is handled as
        // without the suspension - the sweep's resume re-arms and the fault is charged once - and from the next
        // pass the pause holds the view: no seed turn, no work. RESUME WAL lets the sweep complete.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "x");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 50);
        assertMemoryLeak(fault.facade(), () -> {
            setCurrentMicros(0);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = createSeedingView(job);
                fault.of(engine.verifyTableName("base").getDirName());
                final AtomicBoolean isSuspendArmed = new AtomicBoolean(true);
                hooks(job).setRefreshFaultObserver(t -> {
                    if (isSuspendArmed.compareAndSet(true, false)) {
                        suspendLiveViewWal("");
                    }
                });
                fault.armAppliedScan("2026-04-02");
                for (int i = 0; i < SEED_COMPLETION_PASSES && isSuspendArmed.get(); i++) {
                    job.run();
                    drainWalQueue();
                }
                Assert.assertTrue(fault.hasAppliedScanFired());
                Assert.assertFalse("the fault observer must have suspended the view", isSuspendArmed.get());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals("the fault is charged once", 1, instance.getFlushRetryCount());
                Assert.assertFalse("the fault re-arms the sweep's resume", instance.isSeedResumeAttempted());
                final long offset = instance.getSeedDataOffset();
                final long seqTxn = lvTracker().getSeqTxn();

                drivePausePass(job);
                Assert.assertFalse(drivePausePass(job));
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
                Assert.assertEquals(offset, instance.getSeedDataOffset());
                Assert.assertEquals(seqTxn, lvTracker().getSeqTxn());
                Assert.assertEquals(1, instance.getRefreshFaultCount());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveSeedToCompletion(job, "lv");
                drivePastFlushEvery(job);
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                assertPauseReads("resumed", "base");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSuspendedSeedingViewPausesUntilResume() throws Exception {
        // A view still sweeping the base's history when SUSPEND WAL lands holds the sweep: no seed turn runs,
        // its offset and the view's WAL stay put, it stays SEEDING, and the job reports no work. RESUME WAL
        // continues the sweep to ACTIVE.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 50);
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = createSeedingView(job);
                final long offset = instance.getSeedDataOffset();
                final long seqTxn = lvTracker().getSeqTxn();
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                for (int pass = 0; pass < 3; pass++) {
                    Assert.assertFalse("a paused seeding view must report no work", drivePausePass(job));
                }
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
                Assert.assertEquals(offset, instance.getSeedDataOffset());
                Assert.assertEquals(seqTxn, lvTracker().getSeqTxn());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveSeedToCompletion(job, "lv");
                drivePastFlushEvery(job);
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                assertPauseReads("resumed", "base");
                assertNoRefreshFaults("lv");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSuspendedSeedingViewPausesUntilResumeWithMidSeedFault() throws Exception {
        // A one-shot fault armed for a partition the sweep has yet to reach, while the seeding view is paused,
        // fires only in the first seed turn after RESUME WAL. That turn re-arms the sweep's resume as without a
        // pause, and the sweep completes on the recompute with its retry budget cleared.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "x");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 50);
        assertMemoryLeak(fault.facade(), () -> {
            setCurrentMicros(0);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = createSeedingView(job);
                fault.of(engine.verifyTableName("base").getDirName());
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                fault.armAppliedScan("2026-04-03");
                for (int pass = 0; pass < 3; pass++) {
                    Assert.assertFalse(drivePausePass(job));
                }
                Assert.assertFalse("no seed turn ran, so nothing met the fault", fault.hasAppliedScanFired());
                Assert.assertEquals(0, instance.getRefreshFaultCount());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveSeedToCompletion(job, "lv");
                drivePastFlushEvery(job);
                Assert.assertTrue(fault.hasAppliedScanFired());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                assertPauseReads("resumed", "base");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testEndWalSuspensionPauseShiftsTheStreak() throws Exception {
        // The pause's stamp moves a running flush-retry streak's start forward by the pause, measured from
        // the first pass that saw the suspension to the first turn after it, so the pause does not count
        // toward the duration budget. Driven directly on a registered view's instance, since only two workers
        // racing a stamp against a turn reach the clamp.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
            final LiveViewInstance instance = lvInstance();
            // No stamp: nothing moves.
            instance.recordRefreshFailure(10_000_000L);
            instance.endWalSuspensionPause(1_000_000_000L);
            Assert.assertEquals(10_000_000L, instance.getFlushRetryStartUs());
            instance.recordRefreshSuccess();

            // A stamp with no streak running: cleared, nothing to shift, so a later end shifts nothing.
            instance.markWalSuspensionPause(20_000_000L);
            instance.endWalSuspensionPause(1_000_000_000L);
            Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            instance.recordRefreshFailure(1_001_000_000L);
            instance.endWalSuspensionPause(2_000_000_000L);
            Assert.assertEquals(1_001_000_000L, instance.getFlushRetryStartUs());
            instance.recordRefreshSuccess();

            // A stamp after the streak began: the start moves by the pause exactly. The first stamp counts.
            instance.recordRefreshFailure(10_000_000L);
            instance.markWalSuspensionPause(20_000_000L);
            instance.markWalSuspensionPause(30_000_000L);
            instance.endWalSuspensionPause(1_001_000_000L);
            Assert.assertEquals(10_000_000L + (1_001_000_000L - 20_000_000L), instance.getFlushRetryStartUs());
            Assert.assertEquals("the count term is untouched", 1, instance.getFlushRetryCount());
            instance.recordRefreshSuccess();

            // A stamp left before the streak began: the shift is capped at the turn's own time.
            instance.markWalSuspensionPause(5_000_000L);
            instance.recordRefreshFailure(10_000_000L);
            instance.endWalSuspensionPause(1_001_000_000L);
            Assert.assertEquals(1_001_000_000L, instance.getFlushRetryStartUs());
            instance.recordRefreshSuccess();

            // A clock behind the stamp shifts nothing.
            instance.recordRefreshFailure(10_000_000L);
            instance.markWalSuspensionPause(20_000_000L);
            instance.endWalSuspensionPause(15_000_000L);
            Assert.assertEquals(10_000_000L, instance.getFlushRetryStartUs());
            instance.recordRefreshSuccess();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSuspensionPauseDoesNotSpendTheRetryBudget() throws Exception {
        // A drain fault starts a flush-retry streak, and SUSPEND WAL pauses the view long past the duration
        // budget. RESUME WAL continues from where the view stopped: the first turn after it finds the streak
        // as old as it was when the pause began, so a fault there is charged as the streak's next retry
        // instead of spending the budget and invalidating the view. Without a streak, a fault after RESUME
        // starts one of its own. At the default budget and at a short one.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "x");
        fault.reportReadErrno(LiveViewMidDrainFault.ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            final long defaultBudget = configuration.getLiveViewFlushRetryMaxDurationMicros();
            for (long budget : new long[]{defaultBudget, 1_000_000L}) {
                for (int streak = 0; streak < 2; streak++) {
                    final boolean isStreakRunning = streak == 1;
                    final String label = "[budget=" + budget + ", streak=" + isStreakRunning + ']';
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, budget);
                    // The part of the streak before the pause, well inside the budget.
                    final long beforePause = budget / 6;
                    setCurrentMicros(0);
                    createPauseView(PAUSE_SHAPE_LEAD, "TIMESTAMP");
                    fault.of(engine.verifyTableName("base").getDirName());
                    try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                        insertPauseRow("2026-04-01T00:00:00.000000Z", 1, 0);
                        drainWalQueue();
                        drivePastFlushEvery(job);
                        final LiveViewInstance instance = lvInstance();
                        final long faultUs = currentMicros + 10_000_000L;
                        insertPauseRow("2026-04-01T01:00:00.000000Z", 2, 0);
                        drainWalQueue();
                        if (isStreakRunning) {
                            fault.arm(0);
                            setCurrentMicros(faultUs);
                            job.run();
                            Assert.assertTrue(label, fault.hasFired());
                            Assert.assertEquals(label, faultUs, instance.getFlushRetryStartUs());
                            Assert.assertEquals(label, 1, instance.getFlushRetryCount());
                        }
                        execute("ALTER LIVE VIEW lv SUSPEND WAL");
                        // The first pass that sees the suspension; it dequeues the commit's notification when no
                        // faulting turn did.
                        final long heldUs = faultUs + beforePause;
                        setCurrentMicros(heldUs);
                        drainWalQueue();
                        drainJob(job);
                        setCurrentMicros(faultUs + 1_000_000_000L);
                        Assert.assertFalse(label, drainJob(job));
                        execute("ALTER LIVE VIEW lv RESUME WAL");

                        // The first turn after RESUME faults again.
                        final long turnUs = faultUs + 1_000_000_000L + beforePause;
                        fault.arm(0);
                        setCurrentMicros(turnUs);
                        job.run();
                        Assert.assertTrue(label, fault.hasFired());
                        Assert.assertFalse(label + " [reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
                        if (isStreakRunning) {
                            Assert.assertEquals(label, 2, instance.getFlushRetryCount());
                            Assert.assertEquals(label, faultUs + (turnUs - heldUs), instance.getFlushRetryStartUs());
                        } else {
                            Assert.assertEquals(label, 1, instance.getFlushRetryCount());
                            Assert.assertEquals(label, turnUs, instance.getFlushRetryStartUs());
                        }

                        // The fault clears.
                        driveRefreshToQuiescence(job);
                        drivePastFlushEvery(job);
                        Assert.assertFalse(label, instance.isInvalid());
                        Assert.assertEquals(label, Numbers.LONG_NULL, instance.getFlushRetryStartUs());
                        Assert.assertEquals(label, 0, instance.getFlushRetryCount());
                        assertPauseReads(label, "base");
                    } finally {
                        fault.disarmAll();
                    }
                    execute("DROP LIVE VIEW lv");
                    execute("DROP TABLE base");
                }
            }
        });
    }

    @Test
    public void testIdleScanShardsRegistryAcrossWorkers() throws Exception {
        // The idle fallback scan (scanForLaggingViews) is sharded by live-view table id
        // so the pool does O(views) work per sweep instead of O(workers x views) - every
        // worker re-scanning every view. The sharding must (a) assign every view to
        // EXACTLY one worker (no view can drop out of the periodic catch-up scan), and
        // (b) leave a single-worker pool owning everything (single-threaded/test behavior
        // unchanged).
        assertMemoryLeak(() -> {
            final int workerCount = 4;
            final LiveViewRefreshJob[] jobs = new LiveViewRefreshJob[workerCount];
            for (int w = 0; w < workerCount; w++) {
                jobs[w] = new LiveViewRefreshJob(w, workerCount, engine, 1);
            }
            try {
                for (int tableId = 1; tableId <= 200; tableId++) {
                    int owners = 0;
                    for (int w = 0; w < workerCount; w++) {
                        if (jobs[w].ownsViewShard(tableId)) {
                            owners++;
                        }
                    }
                    Assert.assertEquals(
                            "table id " + tableId + " must be owned by exactly one worker",
                            1,
                            owners
                    );
                }
            } finally {
                for (LiveViewRefreshJob job : jobs) {
                    job.close();
                }
            }
            // A single-worker pool owns every view - no sharding, unchanged behavior.
            try (LiveViewRefreshJob solo = new LiveViewRefreshJob(0, 1, engine, 1)) {
                for (int tableId = 1; tableId <= 200; tableId++) {
                    Assert.assertTrue("single-worker pool owns every view", solo.ownsViewShard(tableId));
                }
            }
        });
    }

    @Test
    public void testIdleScanEnumerationIsSharded() throws Exception {
        // The fallback scan ENUMERATES only each worker's shard via getShardedViews(), so the
        // pool copies each view once per sweep (O(views)) instead of every worker copying every
        // view and discarding the non-owned ones (O(workers x views)). Assert the sharded
        // enumeration is a disjoint cover of the whole registry and that, whenever the views
        // span more than one shard, no single worker enumerates all of them.
        assertMemoryLeak(() -> {
            final int viewCount = 12;
            for (int i = 0; i < viewCount; i++) {
                execute("CREATE TABLE base" + i + " (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE LIVE VIEW lv" + i + " FLUSH EVERY 1s START FROM NOW AS "
                        + "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base" + i);
            }
            final LiveViewRegistry registry = engine.getLiveViewRegistry();
            final ObjList<LiveViewInstance> all = new ObjList<>();
            registry.getViews(all);
            Assert.assertTrue("the created live views must be registered", all.size() >= viewCount);

            final int workerCount = 4;
            final IntHashSet residues = new IntHashSet();
            for (int i = 0, n = all.size(); i < n; i++) {
                residues.add(Math.floorMod(all.getQuick(i).getLiveViewToken().getTableId(), workerCount));
            }

            final ObjList<LiveViewInstance> shard = new ObjList<>();
            final IntHashSet seenTableIds = new IntHashSet();
            int total = 0;
            int maxShardSize = 0;
            int nonEmptyShards = 0;
            for (int w = 0; w < workerCount; w++) {
                registry.getShardedViews(shard, w, workerCount);
                if (shard.size() > 0) {
                    nonEmptyShards++;
                }
                maxShardSize = Math.max(maxShardSize, shard.size());
                total += shard.size();
                for (int i = 0, n = shard.size(); i < n; i++) {
                    final int tableId = shard.getQuick(i).getLiveViewToken().getTableId();
                    Assert.assertEquals("a view lands only in its floorMod shard", w, Math.floorMod(tableId, workerCount));
                    Assert.assertTrue("a view must appear in exactly one shard", seenTableIds.add(tableId));
                }
            }
            // Disjoint cover: every registered view appears in exactly one worker's enumeration.
            Assert.assertEquals("shards must cover every registered view exactly once", all.size(), total);
            Assert.assertEquals("one non-empty shard per distinct residue", residues.size(), nonEmptyShards);
            // Sharded, not full copy: whenever the views span multiple shards, no worker sees all.
            if (residues.size() > 1) {
                Assert.assertTrue("no worker may enumerate the whole registry", maxShardSize < all.size());
            }

            // A single-worker pool enumerates everything - unchanged behavior.
            registry.getShardedViews(shard, 0, 1);
            Assert.assertEquals("single-worker pool enumerates every view", all.size(), shard.size());
        });
    }

    @Test
    public void testIdleScanRefreshesOnlyTheOwnedShard() throws Exception {
        // The two sibling tests above pin ownsViewShard() and getShardedViews() in ISOLATION,
        // which is not the same as pinning what production does with them: swap
        // scanForLaggingViews back to registry.getViews() and both stay green, because
        // neither ever runs the job's own scan. This one does - it drives the real
        // LiveViewRefreshJob.run() on ONE worker of a two-worker pool and requires the idle
        // fallback scan to advance that worker's shard and NOTHING else.
        assertMemoryLeak(() -> {
            final int viewCount = 6;
            for (int i = 0; i < viewCount; i++) {
                execute("CREATE TABLE base" + i + " (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE LIVE VIEW lv" + i + " FLUSH EVERY 1s START FROM NOW AS "
                        + "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base" + i);
            }

            // Seed every view with a single-worker pool, which owns all of them, so the
            // shard assertion below measures the idle scan rather than leftover seeding.
            try (LiveViewRefreshJob solo = new LiveViewRefreshJob(0, 1, engine, 1)) {
                for (int i = 0; i < viewCount; i++) {
                    driveSeedToCompletion(solo, "lv" + i);
                }
            }

            final LiveViewRegistry registry = engine.getLiveViewRegistry();
            // 3, not 2: the loop above creates a base table and a live view alternately, so
            // every live view gets an EVEN table id and a two-worker split would hand the
            // whole registry to worker 0 - a vacuous test. Consecutive even ids cover all
            // three residues mod 3, so both shards are non-empty whatever the ids start at.
            final int workerCount = 3;
            final int workerId = 0;

            for (int i = 0; i < viewCount; i++) {
                execute("INSERT INTO base" + i + " (ts, x) VALUES ('2026-05-01T00:00:00.000000Z', 1)");
            }
            drainWalQueue();
            // Empty the notification queue AFTER the commits: the queue-driven path is not
            // sharded (any worker serves any base-table task), so leaving a notification
            // behind would refresh a view this worker does not own and mask the difference.
            // processNotifications() only reaches scanForLaggingViews when the queue is dry.
            engine.getLiveViewStateStore().clear();

            final ObjList<LiveViewInstance> all = new ObjList<>();
            registry.getViews(all);
            final IntHashSet ownedIds = new IntHashSet();
            final IntHashSet foreignIds = new IntHashSet();
            for (int i = 0, n = all.size(); i < n; i++) {
                final int tableId = all.getQuick(i).getLiveViewToken().getTableId();
                if (Math.floorMod(tableId, workerCount) == workerId) {
                    ownedIds.add(tableId);
                } else {
                    foreignIds.add(tableId);
                }
            }
            // Both sides must be non-empty or the assertion proves nothing.
            Assert.assertTrue("the fixture must produce at least one owned view", ownedIds.size() > 0);
            Assert.assertTrue("the fixture must produce at least one foreign view", foreignIds.size() > 0);

            final CharSequenceLongHashMap before = new CharSequenceLongHashMap();
            for (int i = 0; i < viewCount; i++) {
                before.put("lv" + i, registry.getViewInstance("lv" + i).getLastProcessedSeqTxn());
            }

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(workerId, workerCount, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            for (int i = 0; i < viewCount; i++) {
                final String viewName = "lv" + i;
                final LiveViewInstance instance = registry.getViewInstance(viewName);
                final int tableId = instance.getLiveViewToken().getTableId();
                final long advanced = instance.getLastProcessedSeqTxn();
                if (ownedIds.contains(tableId)) {
                    Assert.assertTrue(
                            viewName + " is in worker " + workerId + "'s shard, so the idle scan must advance it",
                            advanced > before.get(viewName)
                    );
                } else {
                    Assert.assertEquals(
                            viewName + " is not in worker " + workerId + "'s shard, so this worker must leave it alone"
                                    + " - an unsharded scan would advance it too",
                            before.get(viewName),
                            advanced
                    );
                }
            }

            for (int i = 0; i < viewCount; i++) {
                execute("DROP LIVE VIEW lv" + i);
            }
        });
    }

    @Test
    public void testNotificationDrainIsBoundedPerRun() throws Exception {
        // A base table under sustained ingestion re-enqueues its refresh task as soon as it is
        // processed, so an unbounded notification drain would let one base table monopolize the
        // shared refresh pool and starve materialized-view jobs and timers. processNotifications()
        // must drain at most MAX_REFRESH_TASKS_PER_RUN tasks per Job.run() and leave the rest for
        // the next scheduler turn. Base tables with no live view make each task a side-effect-free
        // registry fan-out, isolating the drain bound.
        assertMemoryLeak(() -> {
            final LiveViewStateStore store = engine.getLiveViewStateStore();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final int bound = job.maxRefreshTasksPerRun();
                final int overflow = 3;
                final int taskCount = bound + overflow;
                for (int i = 0; i < taskCount; i++) {
                    final String name = "m3base" + i;
                    store.registerBaseTable(name);
                    final TableToken token = new TableToken(name, name, null, 1_000 + i, false, false, false);
                    store.notifyBaseTableCommit(token, 1);
                }

                Assert.assertTrue("the bounded drain still made progress", job.processNotificationsForTest());

                // A single run drained only the bound; the overflow stays queued for the next turn.
                final LiveViewRefreshTask task = new LiveViewRefreshTask();
                int remaining = 0;
                while (store.tryDequeueRefreshTask(task)) {
                    remaining++;
                }
                Assert.assertEquals("one run must drain at most MAX_REFRESH_TASKS_PER_RUN tasks", overflow, remaining);
            }
            store.clear();
        });
    }

    @Test
    public void testLargeTransactionScratchIsNotRetainedAtPeak() throws Exception {
        // drainBaseWal hands a whole base transaction to the worker-owned WalSegmentPageFrameCursor,
        // whose extractTimestamps() grows a reusable 8-bytes-per-row scratch. jumpTo(0) only rewinds
        // the append cursor, so a single outlier transaction would pin its peak for the refresh
        // worker's lifetime. The cursor must release that peak once it exceeds the retained cap. The
        // cap is lowered here so a modest transaction is enough to drive the shrink deterministically.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final WalSegmentPageFrameCursor cursor = job.walFrameCursorForTest();
                final long cap = 128L * 1024;
                cursor.setMaxRetainedExtractedTsBytes(cap);

                // One large transaction: 50k rows -> ~400 KiB of timestamp scratch, well past the cap.
                execute("INSERT INTO base SELECT timestamp_sequence('2026-05-12T00:00:00.000000Z', 1000000), x " +
                        "FROM long_sequence(50000)");
                drainWalQueue();
                drainJob(job);
                final long afterLarge = cursor.extractedTimestampMemCapacity();
                Assert.assertTrue(
                        "the outlier transaction must grow the scratch past the cap [cap=" + cap + ", capacity=" + afterLarge + "]",
                        afterLarge > cap
                );

                // A subsequent small transaction must release the retained peak, not pin it.
                execute("INSERT INTO base VALUES ('2026-06-12T00:00:00.000000Z', 1)");
                drainWalQueue();
                drainJob(job);
                final long afterSmall = cursor.extractedTimestampMemCapacity();
                Assert.assertTrue(
                        "the retained peak must be released on the next frame [afterSmall=" + afterSmall + ", cap=" + cap + "]",
                        afterSmall <= cap
                );
                Assert.assertTrue("the scratch must actually shrink after the outlier", afterSmall < afterLarge);
            }
        });
    }

    @Test
    public void testWalCursorColumnMappingReflectsProjection() throws Exception {
        // WalSegmentPageFrameCursor publishes a ColumnMapping alongside each frame, and for a
        // live view it is NOT the identity: the view's SELECT both reorders and prunes base
        // columns, so SQL output position i names some other base writer index. Nothing on the
        // NATIVE WAL path would notice it going wrong - values resolve through pageAddresses
        // and never consult the mapping; the parquet path is what reads it - so the triples
        // need pinning directly or a wrong mapping ships silently.
        assertMemoryLeak(() -> {
            // Base writer indexes: ts=0, a=1, b=2, c=3.
            execute("CREATE TABLE base (ts TIMESTAMP, a INT, b INT, c INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            // Projects c, a, ts: reordered against the base, with b pruned entirely.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT c, a, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final WalSegmentPageFrameCursor cursor = job.walFrameCursorForTest();
                // Seed first, so the mapping below is the one the incremental raw-WAL drain
                // built rather than anything left over from the seed's own scan.
                driveSeedToCompletion(job, "lv");

                execute("INSERT INTO base (ts, a, b, c) VALUES ('2026-05-12T00:00:00.000000Z', 10, 20, 30)");
                drainWalQueue();
                drainJob(job);

                final ColumnMapping mapping = cursor.getColumnMapping();
                Assert.assertEquals("the mapping covers exactly the projected columns", 3, mapping.getColumnCount());
                Assert.assertEquals("output position 0 reads base column c", 3, mapping.getWriterIndex(0));
                Assert.assertEquals("output position 1 reads base column a", 1, mapping.getWriterIndex(1));
                Assert.assertEquals("output position 2 reads base column ts", 0, mapping.getWriterIndex(2));
                for (int i = 0, n = mapping.getColumnCount(); i < n; i++) {
                    Assert.assertEquals("column index is the SQL output position", i, mapping.getColumnIndex(i));
                    // A WAL segment carries no replacingIndex chain, so the original writer
                    // index is the writer index. Asserting it keeps the third element of the
                    // triple pinned rather than merely present.
                    Assert.assertEquals(
                            "writer and original writer index must agree for a WAL segment",
                            mapping.getWriterIndex(i),
                            mapping.getOriginalWriterIndex(i)
                    );
                }
                // A refresh fault self-heals into a from-base recompute, which never binds
                // this cursor - so without this the drain could have faulted and the query
                // below would still read right off the recompute.
                assertNoRefreshFaults("lv");
            }
            drainWalQueue();

            // Cross-check on the shared columnIndexes: computeFrame derives BOTH the page
            // addresses and the mapping triples from that one list, so wrong values here
            // would indict the projection the mapping is built from. (Not a check on the
            // mapping itself - as noted above, the NATIVE path never reads it.)
            assertQuery("SELECT c, a FROM lv").noLeakCheck().expectSize().returns("c\ta\n" +
                    "30\t10\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testWalCursorSymbolTableContainsNullValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (a SYMBOL, b SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('seed', NULL, '2026-01-01T00:00:00.000000Z')");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT a, b, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final WalSegmentPageFrameCursor cursor = job.walFrameCursorForTest();

                execute("INSERT INTO base VALUES ('x', NULL, '2026-01-01T00:01:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                assertNoRefreshFaults("lv");
                Assert.assertFalse("a has no NULL in its dictionary or transaction", cursor.getSymbolTable(0).containsNullValue());

                execute("INSERT INTO base VALUES (NULL, NULL, '2026-01-01T00:02:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                assertNoRefreshFaults("lv");
                // The cursor retains the transaction's NULL flags after releasing its WAL reader.
                Assert.assertTrue("the transaction introducing NULL must update a's metadata", cursor.getSymbolTable(0).containsNullValue());
                Assert.assertTrue("the NULL-only column must report NULL with no non-NULL symbols", cursor.getSymbolTable(1).containsNullValue());
                try (TableReader reader = getReader("base")) {
                    Assert.assertEquals(0, reader.getSymbolMapReader(1).getSymbolCount());
                }
            }
            drainWalQueue();
            assertQuery("SELECT a, b, rn FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("""
                    a\tb\trn
                    seed\t\t1
                    x\t\t2
                    \t\t3
                    """);
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMultiWorkerPoolConvergesAllViews() throws Exception {
        // End-to-end multi-worker smoke test: a 3-worker pool (each job built with the
        // workerCount=3 constructor) must converge EVERY view. This exercises the
        // workerCount>1 runtime path - the new constructor and the sharded
        // scanForLaggingViews loop executing under multiple workers - and confirms the
        // sharding change breaks neither convergence nor the single-base fan-out.
        //
        // NOTE: coverage here flows mostly through the UNSHARDED notification path
        // (refreshViewsForBaseTable fans a base commit to every view on that base), so
        // this test does NOT by itself isolate the sharded idle scan. The sharding
        // invariant (every view owned by exactly one worker; workerCount==1 owns all) is
        // pinned deterministically by testIdleScanShardsRegistryAcrossWorkers.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final int viewCount = 6;
            for (int v = 0; v < viewCount; v++) {
                execute("CREATE LIVE VIEW lv" + v + " FLUSH EVERY 1s START FROM BEGINNING AS " +
                        "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
            }
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            final int workerCount = 3;
            final LiveViewRefreshJob[] jobs = new LiveViewRefreshJob[workerCount];
            for (int w = 0; w < workerCount; w++) {
                jobs[w] = new LiveViewRefreshJob(w, workerCount, engine, 1);
            }
            try {
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES; pass++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    boolean progressed = false;
                    for (LiveViewRefreshJob job : jobs) {
                        progressed |= drainJob(job);
                    }
                    drainWalQueue();
                    if (!progressed) {
                        break;
                    }
                }
            } finally {
                for (LiveViewRefreshJob job : jobs) {
                    job.close();
                }
            }
            drainWalQueue();

            for (int v = 0; v < viewCount; v++) {
                assertQuery("SELECT count() FROM lv" + v).noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\n2\n");
            }
            for (int v = 0; v < viewCount; v++) {
                execute("DROP LIVE VIEW lv" + v);
            }
        });
    }

    @Test
    public void testTablesReportsLiveView() throws Exception {
        // Locks the tables() discriminator asymmetry (documented in TablesFunctionFactory):
        // a live view is discoverable ONLY via table_type='L'. The matView BOOLEAN is
        // mat-view-only (false here) and there is deliberately no liveView BOOLEAN -
        // materialized views carry both matView=true and table_type='M', but adding a
        // symmetric liveView column would renumber every position-based tables() consumer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            assertQuery("SELECT table_type, matView FROM tables() WHERE table_name = 'lv'")
                    .noLeakCheck().noRandomAccess().returns("table_type\tmatView\nL\tfalse\n");
            // No liveView boolean column exists (the would-be symmetry with matView).
            try {
                execute("SELECT liveView FROM tables() WHERE table_name = 'lv'");
                Assert.fail("tables() must not expose a liveView column");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "Invalid column")
                );
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testTablesShowsLiveViewWithTypeL() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            assertQuery("SELECT table_type FROM tables() WHERE table_name = 'lv'").noLeakCheck().noRandomAccess().returns("table_type\nL\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCatalogueReportsNoDedupOnLiveView() throws Exception {
        // A view with a single SYMBOL PARTITION BY key, created while sparse publication is
        // on (the default), carries (ts, key) dedup keys in its own metadata so that a
        // repair publication can upsert on them. Its forward commits keep rows that share
        // (ts, key), so the catalogue does not present the view as deduplicating:
        // tables().dedup and the upsertKey column of SHOW COLUMNS and table_columns() read
        // false. A plain DEDUP table keeps reporting true.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE dedup_t (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS
                    SELECT ts, sym, x, row_number() OVER w AS rn FROM base
                    WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')
                    """);
            // The view's table metadata keeps the keys a sparse repair publication upserts on;
            // only the catalogue's answer differs.
            try (TableReader reader = engine.getReader("lv")) {
                Assert.assertTrue(reader.getMetadata().isDedupKey(reader.getMetadata().getColumnIndex("ts")));
                Assert.assertTrue(reader.getMetadata().isDedupKey(reader.getMetadata().getColumnIndex("sym")));
            }
            execute("""
                    INSERT INTO base VALUES
                    ('2026-01-01T00:00:00.000000Z', 'a', 1),
                    ('2026-01-01T00:00:00.000000Z', 'a', 2),
                    ('2026-01-01T00:00:00.000000Z', 'b', 3)
                    """);
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();
            // The view holds two rows at ('2026-01-01T00:00:00.000000Z', 'a').
            assertQuery("lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tx\trn
                            2026-01-01T00:00:00.000000Z\ta\t1\t1
                            2026-01-01T00:00:00.000000Z\ta\t2\t2
                            2026-01-01T00:00:00.000000Z\tb\t3\t1
                            """);

            assertQuery("SELECT table_name, dedup, table_type FROM tables() WHERE table_name IN ('lv', 'dedup_t') ORDER BY table_name")
                    .noLeakCheck()
                    .returns("""
                            table_name\tdedup\ttable_type
                            dedup_t\ttrue\tT
                            lv\tfalse\tL
                            """);
            final String expectedViewColumns = """
                    column\tupsertKey
                    ts\tfalse
                    sym\tfalse
                    x\tfalse
                    rn\tfalse
                    """;
            assertQuery("SELECT \"column\", upsertKey FROM (SHOW COLUMNS FROM lv)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expectedViewColumns);
            assertQuery("SELECT \"column\", upsertKey FROM table_columns('lv')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expectedViewColumns);
            final String expectedTableColumns = """
                    column\tupsertKey
                    ts\ttrue
                    sym\ttrue
                    x\tfalse
                    """;
            assertQuery("SELECT \"column\", upsertKey FROM (SHOW COLUMNS FROM dedup_t)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expectedTableColumns);
            assertQuery("SELECT \"column\", upsertKey FROM table_columns('dedup_t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expectedTableColumns);
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInformationSchemaTablesShowsLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            assertQuery("SELECT table_type, is_insertable_into FROM information_schema.tables() " +
                    "WHERE table_name = 'lv'").noLeakCheck().noRandomAccess().returns("table_type\tis_insertable_into\n" +
                    "LIVE VIEW\tfalse\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPgClassReportsLiveViewAsRelkindV() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            assertQuery("SELECT relkind FROM pg_class() WHERE relname = 'lv'").noLeakCheck().noRandomAccess().returns("relkind\nv\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectAlterTable() throws Exception {
        assertMutationRejected(
                "ALTER TABLE lv ADD COLUMN x INT",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectInsertInto() throws Exception {
        assertMutationRejected(
                "INSERT INTO lv VALUES (1, '2026-01-01T00:00:00.000000Z', 1)",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectUpdate() throws Exception {
        assertMutationRejected(
                "UPDATE lv SET val = 0",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectTruncate() throws Exception {
        assertMutationRejected(
                "TRUNCATE TABLE lv",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectReindex() throws Exception {
        assertMutationRejected(
                "REINDEX TABLE lv COLUMN val LOCK EXCLUSIVE",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectVacuum() throws Exception {
        assertMutationRejected(
                "VACUUM TABLE lv",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectRename() throws Exception {
        assertMutationRejected(
                "RENAME TABLE lv TO lv2",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testReplicationRenameRekeysRegistryAndDependentGraph() throws Exception {
        // engine.applyTableRename is the replication apply path's entry point: a downloaded
        // live view whose real name is still taken registers under a pending temp name and
        // moves to the real name here. The LV registry is keyed by name and the dependent
        // graph compares tokens by name, so the rename must re-key both. An instance left
        // under the dead name is one a later drop's removeView(realName) misses: it is never
        // marked dropped, never fenced and never freed, and WalPurgeJob keeps clamping the
        // base WAL purge floor to its frozen watermark for the life of the process.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv_pending FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, sum(x) OVER (PARTITION BY x ORDER BY ts " +
                    "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            final LiveViewRegistry registry = engine.getLiveViewRegistry();
            final LiveViewInstance instance = registry.getViewInstance("lv_pending");
            Assert.assertNotNull(instance);
            final long lifecycleIdentity = instance.getLifecycleIdentity();
            final String lvDirName;

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                final TableToken pendingToken = engine.verifyTableName("lv_pending");
                lvDirName = pendingToken.getDirName();
                engine.applyTableRename(pendingToken, pendingToken.renamed("lv"));

                // The real name finds the same instance, re-pointed at the renamed token,
                // and the pending name is dead.
                Assert.assertSame("the real name must find the renamed instance", instance, registry.getViewInstance("lv"));
                Assert.assertEquals("rename must retain lifecycle identity", lifecycleIdentity, instance.getLifecycleIdentity());
                Assert.assertEquals("lv", instance.getLiveViewToken().getTableName());
                Assert.assertEquals("lv", instance.getDefinition().getViewName());
                Assert.assertNull("the pending name must be dead", registry.getViewInstance("lv_pending"));

                // The dependent graph carries the renamed token, so the drop below can
                // find the entry by token equality, which compares the name too.
                final ObjList<TableToken> dependents = new ObjList<>();
                engine.getDependentViewGraph().getDependentViews(engine.verifyTableName("base"), dependents);
                Assert.assertEquals(1, dependents.size());
                Assert.assertEquals("lv", dependents.getQuick(0).getTableName());
                Assert.assertTrue(dependents.getQuick(0).isLiveView());

                // The renamed view keeps refreshing under its new name.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:01.000000Z', 2)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv")
                        .noRandomAccess()
                        .expectSize()
                        .noLeakCheck()
                        .returns("""
                                count
                                2
                                """);

                // Dropping by the real name tears the renamed instance down: it is fenced
                // and marked dropped, both name maps forget it, and the base's fan-out
                // list - the input WalPurgeJob clamps the base WAL purge floor from - is
                // empty again.
                execute("DROP LIVE VIEW lv");
                Assert.assertTrue("the drop must fence and mark the renamed instance", instance.isDropped());
                Assert.assertNull(registry.getViewInstance("lv"));
                final ObjList<LiveViewInstance> floorSink = new ObjList<>();
                registry.getViewsForBaseTable("base", floorSink);
                Assert.assertEquals(
                        "a dropped renamed view must stop clamping the base WAL purge floor",
                        0,
                        floorSink.size()
                );
                dependents.clear();
                engine.getDependentViewGraph().getDependentViews(engine.verifyTableName("base"), dependents);
                Assert.assertEquals("the dependent graph must forget the dropped renamed view", 0, dependents.size());
            }

            // The dropped view's token and files are reclaimed by the standard WAL
            // machinery, exactly as for a view that was never renamed.
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                for (int i = 0; i < 8; i++) {
                    drainWalQueue();
                    purgeJob.drain(0);
                }
            }
            Assert.assertNull(
                    "the dropped live view's token must be gone from the name registry",
                    engine.getTableTokenIfExists("lv")
            );
            final Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot());
            Assert.assertNotEquals(
                    "the dropped live view's table files must be purged",
                    TableUtils.TABLE_EXISTS,
                    TableUtils.exists(
                            engine.getConfiguration().getFilesFacade(),
                            path,
                            engine.getConfiguration().getDbRoot(),
                            lvDirName
                    )
            );
        });
    }

    @Test
    public void testRejectDropLiveViewOnPlainTable() throws Exception {
        // DROP LIVE VIEW must refuse a name that is not a live view. The gate is
        // kind-agnostic (a single !isLiveView() check), so a plain table produces the
        // "live view name expected [name=...]" reject - distinct from DROP TABLE /
        // DROP MATERIALIZED VIEW on a live view (tested above), which name the offending
        // kind. A missing name instead yields "live view does not exist".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            try {
                execute("DROP LIVE VIEW t");
                Assert.fail("expected DROP LIVE VIEW to reject a plain table name");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view name expected [name=t]")
                );
            }
            execute("DROP TABLE t");
        });
    }

    @Test
    public void testRejectDirectEngineDropLiveViewOnPlainTable() throws Exception {
        // The public CairoEngine.dropLiveView API is reachable directly, bypassing the
        // SQL compiler's kind guard (executeDropLiveView). It must refuse a non-live-view
        // token BEFORE any authorization, sentinel, registry or filesystem mutation -
        // otherwise a caller (even with a drop-denying SecurityContext) reaches the
        // generic, authorization-free teardown and deletes an ordinary table. The
        // denying context here never even gets consulted: the kind check fires first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            final TableToken tokenBefore = engine.getTableTokenIfExists("t");
            Assert.assertNotNull(tokenBefore);
            try {
                engine.dropLiveView("t", ReadOnlySecurityContext.INSTANCE);
                Assert.fail("expected dropLiveView to reject a plain table name");
            } catch (CairoException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view name expected [name=t]")
                );
            }
            // The ordinary table must still be registered and droppable through the
            // normal path - proof that dropLiveView tore nothing down.
            final TableToken tokenAfter = engine.getTableTokenIfExists("t");
            Assert.assertNotNull("dropLiveView must not delete the ordinary table", tokenAfter);
            Assert.assertEquals(tokenBefore, tokenAfter);
            execute("DROP TABLE t");
        });
    }

    @Test
    public void testRejectDropTableOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP TABLE lv",
                "table name expected, got live view name: lv"
        );
    }

    @Test
    public void testRejectDropViewOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP VIEW lv",
                "view name expected, got live view name"
        );
    }

    @Test
    public void testRejectDropMaterializedViewOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP MATERIALIZED VIEW lv",
                "materialized view name expected, got live view name"
        );
    }

    @Test
    public void testRebaseWalRejectsLiveViewToken() throws Exception {
        // rebaseWalTable0's kind guard read isView(), which is Type.VIEW exactly, and a
        // live view is a WAL table of a different kind - so it walked past the guard and
        // only the SQL layer stood between it and a rebase that mints a new dir and table
        // id under a registry entry, refresh state and _lv files that still name the old
        // one. Asserting the message rather than "it threw" is what separates the guard
        // from the suspension check below it, which is where the token used to land.
        //
        // Mat views must keep working: rebasing one is supported
        // (MatViewTest#testRebaseWalMaterializedView), so the plain table and the mat view
        // both have to reach that suspension check rather than the kind guard.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            execute("CREATE MATERIALIZED VIEW mv AS (" +
                    "SELECT ts, avg(val) AS av FROM base SAMPLE BY 1h) PARTITION BY DAY");

            final TableToken lvToken = engine.verifyTableName("lv");
            Assert.assertEquals(TableToken.Type.LIVE_VIEW, lvToken.getType());
            Assert.assertTrue("a live view is a WAL table, which is what made the old guard miss it", lvToken.isWal());
            try {
                engine.rebaseWalTable(lvToken);
                Assert.fail("REBASE WAL must be refused for a live view");
            } catch (CairoException e) {
                Assert.assertTrue(
                        "expected the kind guard, got: " + e.getFlyweightMessage(),
                        Chars.contains(e.getFlyweightMessage(), "REBASE WAL is supported only for WAL tables")
                );
            }

            final TableToken mvToken = engine.verifyTableName("mv");
            Assert.assertEquals(TableToken.Type.MAT_VIEW, mvToken.getType());
            assertRebaseWalReachesSuspensionCheck(mvToken);
            assertRebaseWalReachesSuspensionCheck(engine.verifyTableName("base"));

            execute("DROP MATERIALIZED VIEW mv");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsRendersUnsetTimestampsAsNull() throws Exception {
        // A view that has never refreshed holds no checkpoint generation and, under
        // START FROM BEGINNING, no lower bound either. Every one of these columns must
        // read NULL - a 0 would render as 1970-01-01 on the TIMESTAMP ones and as a
        // legitimate-looking count on the rest. The query runs twice so the second pass
        // goes through the cached factory after the cursor released the first walk's
        // state.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            final String query = "SELECT view_name, view_lower_bound_timestamp, checkpoint_timeline_generation, " +
                    "checkpoint_timeline_entries, checkpoint_repair_correction_timestamp, " +
                    "checkpoint_repair_high_timestamp, checkpoint_last_write_micros " +
                    "FROM live_views() WHERE view_name = 'lv'";
            // A NULL TIMESTAMP prints as an empty field, a NULL LONG as "null".
            final String expected = """
                    view_name\tview_lower_bound_timestamp\tcheckpoint_timeline_generation\tcheckpoint_timeline_entries\tcheckpoint_repair_correction_timestamp\tcheckpoint_repair_high_timestamp\tcheckpoint_last_write_micros
                    lv\t\tnull\tnull\t\t\tnull
                    """;
            // noCircuitBreakerCheck / noLeakCheck as everywhere else this catalogue is
            // asserted: it walks the in-memory registry and performs no per-row checks.
            assertQuery(query).noLeakCheck().noRandomAccess().noCircuitBreakerCheck().returns(expected);
            assertQuery(query).noLeakCheck().noRandomAccess().noCircuitBreakerCheck().returns(expected);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateOverDedupBaseSucceeds() throws Exception {
        // A DEDUP base table is no longer rejected at CREATE; the refresh worker
        // routes it onto the coupled, applied-reader path. See
        // LiveViewDedupBaseTest for the correctness suite.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateTableLikeLiveViewKeepsRowsSharingUpsertKey() throws Exception {
        // A live view's upsert keys name the pair a sparse repair publication upserts on,
        // not a uniqueness guarantee: the view emits two rows with the same (ts, sym)
        // whenever its base does, and its own forward commits do not deduplicate them.
        // A plain table copied from the view must not inherit the keys, or copying the
        // view's rows into it collapses those rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS
                    SELECT ts, sym, x, row_number() OVER w AS rn FROM base
                    WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')
                    """);
            execute("""
                    INSERT INTO base VALUES
                    ('2026-01-01T00:00:00.000000Z', 'a', 1),
                    ('2026-01-01T00:00:00.000000Z', 'a', 2),
                    ('2026-01-01T00:00:00.000000Z', 'b', 3),
                    ('2026-01-01T00:01:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            // The view's table metadata still carries (ts, sym) as its dedup keys; the catalogue
            // does not report them (see testCatalogueReportsNoDedupOnLiveView).
            try (TableReader reader = engine.getReader("lv")) {
                Assert.assertTrue(reader.getMetadata().isDedupKey(reader.getMetadata().getColumnIndex("ts")));
                Assert.assertTrue(reader.getMetadata().isDedupKey(reader.getMetadata().getColumnIndex("sym")));
                Assert.assertFalse(reader.getMetadata().isDedupKey(reader.getMetadata().getColumnIndex("x")));
                Assert.assertFalse(reader.getMetadata().isDedupKey(reader.getMetadata().getColumnIndex("rn")));
            }
            final String expectedRows = """
                    ts\tsym\tx\trn
                    2026-01-01T00:00:00.000000Z\ta\t1\t1
                    2026-01-01T00:00:00.000000Z\ta\t2\t2
                    2026-01-01T00:00:00.000000Z\tb\t3\t1
                    2026-01-01T00:01:00.000000Z\ta\t4\t3
                    """;
            assertQuery("lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expectedRows);

            execute("CREATE TABLE t (LIKE lv)");
            assertQuery("SHOW CREATE TABLE t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 't' (\s
                            \tts TIMESTAMP,
                            \tsym SYMBOL,
                            \tx LONG,
                            \trn LONG
                            ) timestamp(ts) PARTITION BY DAY;
                            """);
            execute("INSERT INTO t SELECT * FROM lv");
            drainWalQueue();
            assertQuery("t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expectedRows);

            // CREATE TABLE AS SELECT takes its dedup keys from its own DEDUP clause only.
            execute("CREATE TABLE t2 AS (SELECT * FROM lv) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertQuery("SHOW CREATE TABLE t2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 't2' (\s
                            \tts TIMESTAMP,
                            \tsym SYMBOL,
                            \tx LONG,
                            \trn LONG
                            ) timestamp(ts) PARTITION BY DAY;
                            """);
            assertQuery("t2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expectedRows);
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectMissingWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT val, ts FROM base");
                Assert.fail("expected SqlException for missing window function");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view select must contain at least one window function")
                );
            }
        });
    }

    @Test
    public void testRejectNonSnapshotCapableWindowFunction() {
        // Drive the validation directly with a ZERO_PASS stub lacking snapshot support to
        // pin the exact user-facing message and position deterministically. The same reject
        // also fires through real SQL for un-partitioned aggregate windows - see
        // testRejectUnpartitionedAggregateWindowFunction - but the stub gives a stable
        // function name (test_no_snapshot) and an explicit position to assert on.
        try {
            CairoEngine.validateLiveViewWindowFunction(new NonSnapshotWindowFunction(), 42);
            Assert.fail("expected SqlException for non-snapshot-capable window function");
        } catch (SqlException e) {
            Assert.assertEquals(42, e.getPosition());
            Assert.assertTrue(
                    e.getMessage(),
                    e.getMessage().contains("live view select cannot use window function test_no_snapshot(); incremental snapshot is not supported for this function yet")
            );
        }
    }

    @Test
    public void testRejectSnapshotCapableWindowFunctionWithoutCompilerMetadata() {
        try {
            CairoEngine.validateLiveViewWindowFunction(new SnapshotWindowFunctionWithoutMetadata(), 43);
            Assert.fail("expected SqlException for missing checkpoint compiler metadata");
        } catch (SqlException e) {
            Assert.assertEquals(43, e.getPosition());
            Assert.assertTrue(
                    e.getMessage(),
                    e.getMessage().contains("live view checkpoint compiler metadata is missing for window function test_no_metadata()")
            );
        }
    }

    @Test
    public void testRejectUnpartitionedAggregateWindowFunction() throws Exception {
        // An un-partitioned aggregate window (no PARTITION BY) is ZERO_PASS but has no
        // partition Map to snapshot, so it is not live-view-eligible and stays rejected
        // by nature - unlike the per-type migration train, this shape is never migratable.
        // It clears the pass-count check and hits the supportsCheckpointState() reject in
        // validateLiveViewWindowFunction, exercising the real-SQL path the stub-driven
        // testRejectNonSnapshotCapableWindowFunction cannot reach. Adding a PARTITION BY to
        // the same query is accepted (the partitioned ZERO_PASS aggregate shapes are
        // migrated), so the missing partition is the sole reason these reject.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // avg() over a bounded ROWS frame with no PARTITION BY.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                        "SELECT ts, sym, avg(v) OVER w AS a FROM base " +
                        "WINDOW w AS (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");
                Assert.fail("expected SqlException for un-partitioned avg() window function");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("incremental snapshot is not supported for this function yet")
                );
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("avg"));
            }

            // sum() over a bounded RANGE frame with no PARTITION BY - same reject.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                        "SELECT ts, sym, sum(v) OVER w AS a FROM base " +
                        "WINDOW w AS (ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW)");
                Assert.fail("expected SqlException for un-partitioned sum() window function");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("incremental snapshot is not supported for this function yet")
                );
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("sum"));
            }
        });
    }

    @Test
    public void testRejectTwoPassWindowFunction() throws Exception {
        // TWO_PASS window functions - incremental refresh cannot drive them because the second pass
        // needs the partition's total row count up front. ntile() was the only one covered; cume_dist
        // and percent_rank are the other two an ordinary user is likely to reach for, and neither had
        // any live view coverage at all (validateLiveViewWindowFunction's own comment names
        // percent_rank as the example, so it is worth pinning that it really is refused).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            assertTwoPassWindowFunctionRejected("ntile(4)");
            assertTwoPassWindowFunctionRejected("cume_dist()");
            assertTwoPassWindowFunctionRejected("percent_rank()");
        });
    }

    @Test
    public void testRejectJoinInLiveViewSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base1 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE TABLE base2 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // The JOIN-shaped factory tree fails the validateLiveViewFactory check
            // that requires a single WAL base table at the leaf.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                        "SELECT base1.val, base1.ts, count(*) OVER (PARTITION BY 0 ORDER BY base1.ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base1 " +
                        "JOIN base2 ON base1.ts = base2.ts");
                Assert.fail("expected SqlException for JOIN in live view select");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "expected an LV-related rejection, got: " + e.getMessage(),
                        e.getMessage().contains("live view") || e.getMessage().contains("simple scan")
                );
            }
        });
    }

    @Test
    public void testRefreshWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE val > 5");
            // Mix in rows that fail the filter (val <= 5) — they must NOT advance rn.
            execute("INSERT INTO base (val, ts) VALUES " +
                    "(1, '2026-01-01T00:00:00.000000Z'), " +
                    "(10, '2026-01-01T00:01:00.000000Z'), " +
                    "(3, '2026-01-01T00:02:00.000000Z'), " +
                    "(20, '2026-01-01T00:03:00.000000Z'), " +
                    "(30, '2026-01-01T00:04:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("val\tts\trn\n" +
                    "10\t2026-01-01T00:01:00.000000Z\t1\n" +
                    "20\t2026-01-01T00:03:00.000000Z\t2\n" +
                    "30\t2026-01-01T00:04:00.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshWithSymbolColumnComparisonHandlesNullOnlyColumn() throws Exception {
        // Edge-case coverage for raw-WAL SYMBOL equality: the right column b carries a
        // committed NULL but zero distinct non-NULL symbols before the view exists, and the
        // incremental transaction then writes only NULLs into b. The frame must still expose
        // VALUE_IS_NULL so the (NULL, NULL) row matches under '='. EqSymFunctionFactory compares
        // the keys directly; this test does not exercise containsNullValue().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (a SYMBOL, b SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // Applied before the view exists: b's committed dictionary carries a null with no
            // non-null symbols; a gets a real dictionary entry.
            execute("INSERT INTO base (a, b, val, ts) VALUES ('seed', NULL, 1, '2026-01-01T00:00:00.000000Z')");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv_eq FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT a, b, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE a = b");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv_eq");
                // Incremental txn writes only nulls into b (no new distinct symbol), with a null a too.
                execute("INSERT INTO base (a, b, val, ts) VALUES (NULL, NULL, 2, '2026-01-01T00:01:00.000000Z')");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            assertQuery("SELECT a, b, val FROM base WHERE a = b ORDER BY ts").noLeakCheck().returns("a\tb\tval\n" +
                    "\t\t2\n");
            assertQuery("SELECT a, b, val FROM lv_eq ORDER BY ts").noLeakCheck().expectSize().returns("a\tb\tval\n" +
                    "\t\t2\n");
            assertNoRefreshFaults("lv_eq");

            execute("DROP LIVE VIEW lv_eq");
        });
    }

    @Test
    public void testRefreshWithSymbolColumnComparisonHandlesNulls() throws Exception {
        // Regression coverage for raw-WAL SYMBOL-column equality during incremental refresh.
        // EqSymFunctionFactory translates non-NULL values between the two symbol domains and
        // compares NULL through the VALUE_IS_NULL key directly. It does not consult
        // containsNullValue(). These equality assertions therefore pin NULL key semantics, not
        // consumption of WalSegmentPageFrameCursor.WalSymbolTable's NULL-domain metadata.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (a SYMBOL, b SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // Applied before the views exist, seeding the clean dictionaries for a and b.
            execute("INSERT INTO base (a, b, val, ts) VALUES " +
                    "('x', 'x', 1, '2026-01-01T00:00:00.000000Z'), " +
                    "('x', 'y', 2, '2026-01-01T00:01:00.000000Z')");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv_eq FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT a, b, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE a = b");
            execute("CREATE LIVE VIEW lv_ne FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT a, b, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE a != b");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Finish seeding from the applied base FIRST, so the (NULL, NULL) rows below arrive
                // only through incremental refresh. Otherwise the seed reads them from the base table
                // via its real symbol reader and never exercises WalSymbolTable.
                driveSeedToCompletion(job, "lv_eq");
                driveSeedToCompletion(job, "lv_ne");

                // These commits arrive after the seed, so incremental refresh reads them straight
                // from the WAL segment through WalSymbolTable rather than a recompute of the applied
                // base. The right column b carries NULLs (val=3, val=4), so the raw frame must
                // expose VALUE_IS_NULL for the (NULL, NULL) row to match under '='.
                execute("INSERT INTO base (a, b, val, ts) VALUES " +
                        "(NULL, NULL, 3, '2026-01-01T00:02:00.000000Z'), " +
                        "('x', NULL, 4, '2026-01-01T00:03:00.000000Z')");
                execute("INSERT INTO base (a, b, val, ts) VALUES " +
                        "('y', 'y', 5, '2026-01-01T00:04:00.000000Z'), " +
                        "(NULL, 'y', 6, '2026-01-01T00:05:00.000000Z'), " +
                        "('z', 'z', 7, '2026-01-01T00:06:00.000000Z')");
                drainWalQueue();

                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            // Ground truth: the base SELECT itself. QuestDB SYMBOL equality treats NULL = NULL as
            // true, so a = b keeps (NULL, NULL) and a != b drops it.
            assertQuery("SELECT a, b, val FROM base WHERE a = b ORDER BY ts").noLeakCheck().returns("a\tb\tval\n" +
                    "x\tx\t1\n" +
                    "\t\t3\n" +
                    "y\ty\t5\n" +
                    "z\tz\t7\n");
            // The live view refreshed off the raw WAL must agree with it, row for row.
            assertQuery("SELECT a, b, val FROM lv_eq ORDER BY ts").noLeakCheck().expectSize().returns("a\tb\tval\n" +
                    "x\tx\t1\n" +
                    "\t\t3\n" +
                    "y\ty\t5\n" +
                    "z\tz\t7\n");

            assertQuery("SELECT a, b, val FROM base WHERE a != b ORDER BY ts").noLeakCheck().returns("a\tb\tval\n" +
                    "x\ty\t2\n" +
                    "x\t\t4\n" +
                    "\ty\t6\n");
            assertQuery("SELECT a, b, val FROM lv_ne ORDER BY ts").noLeakCheck().expectSize().returns("a\tb\tval\n" +
                    "x\ty\t2\n" +
                    "x\t\t4\n" +
                    "\ty\t6\n");

            // The incremental raw-WAL path, not a recompute, must have produced these rows: a refresh
            // fault self-heals into a full recompute from the applied base (correct symbol tables),
            // which would mask a WalSymbolTable defect.
            assertNoRefreshFaults("lv_eq");
            assertNoRefreshFaults("lv_ne");

            execute("DROP LIVE VIEW lv_eq");
            execute("DROP LIVE VIEW lv_ne");
        });
    }

    @Test
    public void testRefreshWithSymbolFilterUnderJitDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        assertSymbolEqualityFilterRefreshes();
    }

    @Test
    public void testRefreshWithSymbolFilterUnderJitEnabled() throws Exception {
        // Together with the JIT-disabled twin this requires the same refreshed rows under both JIT
        // modes. Every refresh path in LiveViewRefreshJob applies the Java residual filter, never the
        // compiled one, so neither test exercises the JIT symbol bind variables.
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_ENABLED));
        assertSymbolEqualityFilterRefreshes();
    }

    @Test
    public void testRefreshWithSymbolInConstantSetHandlesNulls() throws Exception {
        // Regression for the raw-WAL live view symbol table's keyOf(null). A residual filter of the
        // form "sym IN ('A', NULL)" runs through InSymbolFunctionFactory.Func.init(), which resolves
        // every constant in the set - the NULL literal included - to an int key once per transaction
        // and then matches rows with IntHashSet.contains(record.getInt(...)). WalSegmentPageFrameCursor's
        // WalSymbolTable used to answer keyOf(null) with VALUE_NOT_FOUND rather than the VALUE_IS_NULL
        // that SymbolMapReaderImpl and EmptySymbolMapReader return, so the key set never held the null
        // key while WalWriter stores exactly VALUE_IS_NULL for a null symbol. Every NULL row seen
        // through incremental refresh therefore failed the filter and the view diverged permanently
        // from the equivalent base query.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // Applied before the view exists, seeding the clean dictionary with 'A' and 'B'.
            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('A', 1, '2026-01-01T00:00:00.000000Z'), " +
                    "('B', 2, '2026-01-01T00:01:00.000000Z')");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym IN ('A', NULL)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Finish seeding from the applied base FIRST, so the rows below reach the view only
                // through incremental refresh. Otherwise the seed resolves the constants against the
                // base table's real symbol reader and never exercises WalSymbolTable.
                driveSeedToCompletion(job, "lv");

                // Two commits, so the constants are re-resolved per transaction. 'C' is new to the
                // dictionary and lands in this txn's diff band above the clean symbol count, which
                // keeps the overlay probe in play alongside the null key.
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "(NULL, 3, '2026-01-01T00:02:00.000000Z'), " +
                        "('B', 4, '2026-01-01T00:03:00.000000Z')");
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('A', 5, '2026-01-01T00:04:00.000000Z'), " +
                        "('C', 6, '2026-01-01T00:05:00.000000Z'), " +
                        "(NULL, 7, '2026-01-01T00:06:00.000000Z')");
                drainWalQueue();

                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            // Ground truth: the base SELECT itself. QuestDB resolves the NULL literal to the null
            // symbol key, so IN ('A', NULL) keeps the NULL rows and drops 'B' and 'C'.
            assertQuery("SELECT sym, val FROM base WHERE sym IN ('A', NULL) ORDER BY ts").noLeakCheck().returns("sym\tval\n" +
                    "A\t1\n" +
                    "\t3\n" +
                    "A\t5\n" +
                    "\t7\n");
            // The live view refreshed off the raw WAL must agree with it, row for row.
            assertQuery("SELECT sym, val FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\n" +
                    "A\t1\n" +
                    "\t3\n" +
                    "A\t5\n" +
                    "\t7\n");

            // The incremental raw-WAL path, not a recompute, must have produced these rows: a refresh
            // fault self-heals into a full recompute from the applied base (correct symbol tables),
            // which would mask a WalSymbolTable defect.
            assertNoRefreshFaults("lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshWithWhereClauseOnIndexedSymbol() throws Exception {
        // C1 regression. An equality filter on an INDEXED symbol used to be pushed into a
        // DeferredSingleSymbolFilterPageFrameRecordCursorFactory whose predicate lives in the
        // row cursor, invisible to the incremental refresh path (which applies only the residual
        // filter Function). Before the fix the view admitted every base row - including sym='b' -
        // because the intended WhereClauseParser.useIndexedSymbolFilters guard was never read.
        // Suppressing indexed-symbol key extraction during live view compilation now leaves the
        // predicate as a residual filter the refresh applies, so only sym='a' rows survive and rn
        // advances only for survivors (identical to the non-indexed WHERE val > 5 case above).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL INDEX, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym = 'a'");
            // Interleave a/b rows so a leaked 'b' row would perturb both the row set and rn.
            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('a', 1, '2026-01-01T00:00:00.000000Z'), " +
                    "('b', 2, '2026-01-01T00:01:00.000000Z'), " +
                    "('a', 3, '2026-01-01T00:02:00.000000Z'), " +
                    "('b', 4, '2026-01-01T00:03:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("sym\tval\tts\trn\n" +
                    "a\t1\t2026-01-01T00:00:00.000000Z\t1\n" +
                    "a\t3\t2026-01-01T00:02:00.000000Z\t2\n");
            // Explicit: not a single excluded row slipped through.
            assertQuery("SELECT count() FROM lv WHERE sym <> 'a'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshWithPatternFilterOnIndexedSymbol() throws Exception {
        // C2 regression, and the sharpest of the two shapes: here CREATE ACCEPTS the view and
        // every refresh then throws.
        //
        // A LIKE/ILIKE/~ conjunct on an INDEXED symbol routes code generation into
        // AdaptiveSymbolPatternRecordCursorFactory, which the live view refresh cannot drive:
        // its scan leaf reads through a NonOwningPartitionFrameCursorFactory rather than a full
        // partition scan, so the O3 replay's getCursorInTimestampRange() rejects it, and the
        // forward WAL-segment path evaluates the factory's PreparedSymbolPatternFilter, whose
        // matched-key provider only prepare() initializes - the refresh calls init(), so getBool
        // trips its hasPreparedKeySet assert. Either fault burns the flush retry budget and
        // leaves the view invalid.
        //
        // LiveViewCompiledPlan.of() used to reject the shape at CREATE, because the adaptive
        // factory answered getFilter() == null and the decomposition bottomed out on a
        // non-page-frame node. Once the factory started answering getFilter()/getBaseFactory()
        // - the contract a parallel parent reads to steal its filter - the decomposition
        // descended straight past it onto the scan delegate's page-frame leaf and CREATE passed.
        //
        // The trailing ORDER BY ts is what keeps the window streaming rather than cached: the
        // order-by advice makes the index route use its heap row cursor, so the adaptive factory
        // reports SCAN_DIRECTION_FORWARD and no sort is planned. Without it the planner emits a
        // CachedWindowLight the live view rejects for an unrelated reason, which is the shape
        // testRefreshWithPatternFilterOnIndexedSymbolCachedWindowShape covers.
        //
        // The gate now skips the symbol-pattern index for a live view compile, exactly as
        // WhereClauseParser suppresses indexed-symbol key extraction there, so the planner emits
        // the plain filter-over-full-scan shape the refresh path handles - the same shape the
        // unindexed twin testRefreshWithPatternFilterOnSymbol already gets.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL INDEX, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            final String viewSql = "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn " +
                    "FROM base WHERE sym LIKE 'a%' ORDER BY ts";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // In-order rows: the forward incremental path, which reads the raw WAL segment
                // and applies the compiled plan's residual filter row by row.
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('aaa', 1, '2026-01-01T00:00:00.000000Z'), " +
                        "('bbb', 2, '2026-01-01T00:01:00.000000Z'), " +
                        "('abc', 3, '2026-01-01T00:03:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                // An out-of-order row routes the next cycle through the O3 replay, which scans
                // the base with pageFrameFactory.getCursorInTimestampRange() - the call that
                // rejects a non-full-scan leaf.
                execute("INSERT INTO base (sym, val, ts) VALUES ('axx', 4, '2026-01-01T00:02:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            // rn advances only for survivors, so a leaked or dropped row perturbs it too.
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                    sym\tval\tts\trn
                    aaa\t1\t2026-01-01T00:00:00.000000Z\t1
                    axx\t4\t2026-01-01T00:02:00.000000Z\t2
                    abc\t3\t2026-01-01T00:03:00.000000Z\t3
                    """);
            // Explicit: not a single excluded row slipped through.
            assertQuery("SELECT count() FROM lv WHERE sym NOT LIKE 'a%'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            // A refresh that threw would self-heal into a full recompute from the applied base,
            // which a row-level oracle cannot tell apart from a clean run.
            assertNoRefreshFaults("lv");
            assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            // The mechanism behind all of the above: the live view compile must not plan the
            // adaptive factory at all. EXPLAIN of a CREATE arms the same live-view compile flag
            // the CREATE itself does, and creates nothing.
            assertQuery("CREATE LIVE VIEW lv_plan FLUSH EVERY 1s START FROM NOW AS " + viewSql)
                    .noLeakCheck()
                    .assertsPlanNotContaining("AdaptiveSymbolPattern", "SymbolPatternIndex");
            // ... and the suppression reaches no further than that compile: an ordinary query
            // over the same predicate on the same column still takes the index route.
            assertQuery("SELECT sym, val FROM base WHERE sym LIKE 'a%' ORDER BY ts")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshWithPatternFilterOnIndexedSymbolCachedWindowShape() throws Exception {
        // C2 regression, second shape. Same fixture as
        // testRefreshWithPatternFilterOnIndexedSymbol without the trailing ORDER BY ts: the
        // index route then drains key by key, the adaptive factory reports SCAN_DIRECTION_OTHER,
        // and the planner sorts for the window - so CREATE failed at the cached-window reject
        // ("live view select may only use window functions that support incremental refresh")
        // rather than at the filter decomposition. A loud reject rather than an invalidated
        // view, but still a shape that works without the index and must work with it.
        //
        // Suppressing the symbol-pattern index under a live view compile removes both rejects at
        // once: the plan is the plain filter-over-full-scan the unindexed twin already gets, so
        // the window streams and the refresh drives it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL INDEX, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym LIKE 'a%'");
            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('aaa', 1, '2026-01-01T00:00:00.000000Z'), " +
                    "('bbb', 2, '2026-01-01T00:01:00.000000Z'), " +
                    "('abc', 3, '2026-01-01T00:02:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                    sym\tval\tts\trn
                    aaa\t1\t2026-01-01T00:00:00.000000Z\t1
                    abc\t3\t2026-01-01T00:02:00.000000Z\t2
                    """);
            // A refresh that threw would self-heal into a full recompute from the applied base,
            // which a row-level oracle cannot tell apart from a clean run.
            assertNoRefreshFaults("lv");
            assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshWithPatternFilterOnSymbol() throws Exception {
        // C2 regression. LIKE/ILIKE/~ on a SYMBOL column compiles to a residual filter that
        // pre-resolves its matching keys by enumerating 0..getSymbolCount()-1 at every filter
        // init - and the refresh re-inits the filter once per commit. WalSymbolTable used to
        // answer getSymbolCount() with an Integer.MAX_VALUE upper-bound sentinel, which sent
        // those loops far past the real keys, where valueOf returns null: the contains and
        // regex variants NPE'd (bricking the view as "flush retry budget exhausted" once the
        // retry budget ran out), while the null-safe startsWith/endsWith variants ran 2^31
        // iterations per commit and pinned the shared refresh worker. Both shapes passed
        // CREATE, since validateLiveViewFactory never inspects the residual filter Function.
        //
        // The symbol table now reports its real, finite count, so every shape resolves the
        // keys it actually has. The three commits below are the case that count has to get
        // right: commit 1 is applied first, so its symbols become the base's clean dictionary,
        // and commits 2 and 3 then stay un-applied - the WAL writer restarts local symbol ids
        // at cleanSymbolCount for each commit, so 'xax' and 'zzz' are handed the same key and
        // only the per-txn overlay tells them apart. A count that cut either band short would
        // silently drop the rows keyed past it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // One view per residual-filter shape: contains, startsWith and endsWith are
            // distinct functions with distinct loops, and the '_' wildcard and ~ take the
            // java.util.regex Matcher path, which NPEs on a null value rather than skipping it.
            execute("CREATE LIVE VIEW lv_contains FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym LIKE '%a%'");
            execute("CREATE LIVE VIEW lv_starts FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym LIKE 'a%'");
            execute("CREATE LIVE VIEW lv_ends FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym LIKE '%a'");
            execute("CREATE LIVE VIEW lv_ilike FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym ILIKE '%A%'");
            execute("CREATE LIVE VIEW lv_wildcard FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym LIKE 'x_x'");
            execute("CREATE LIVE VIEW lv_regex FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym ~ 'a'");

            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('aaa', 1, '2026-01-01T00:00:00.000000Z'), " +
                    "('bbb', 2, '2026-01-01T00:01:00.000000Z')");
            drainWalQueue();
            // Commits 2 and 3 are both written while the base dictionary still holds only
            // commit 1's symbols, so each restarts its local ids at cleanSymbolCount=2 and
            // 'xax' and 'zzz' are handed the very same key - a collision baked into the
            // segment at write time, which only the per-txn overlay can tell apart.
            execute("INSERT INTO base (sym, val, ts) VALUES ('xax', 3, '2026-01-01T00:02:00.000000Z')");
            // A null symbol shares the commit with the colliding key: null resolves to no key
            // at all, so an enumeration that walks past the real count meets it as a hole.
            execute("INSERT INTO base (sym, val, ts) VALUES " +
                    "('zzz', 4, '2026-01-01T00:03:00.000000Z'), " +
                    "('aaa', 5, '2026-01-01T00:04:00.000000Z'), " +
                    "(NULL, 6, '2026-01-01T00:05:00.000000Z')");
            drainWalQueue();
            // All three commits are written before the first refresh, so the collision above is
            // already baked into the segment. Each refresh cycle then flushes at most one batch
            // (FLUSH EVERY 1s against the pinned clock), so step the clock past the rate limit
            // until every batch has drained.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 1; i <= 4; i++) {
                    drainJob(job);
                    drainWalQueue();
                    setCurrentMicros(i * 2_000_000L);
                }
            }
            drainWalQueue();

            // rn advances only for survivors, so a leaked or dropped row perturbs it too.
            assertQuery("SELECT sym, val, rn FROM lv_contains ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\trn\n" +
                    "aaa\t1\t1\n" +
                    "xax\t3\t2\n" +
                    "aaa\t5\t3\n");
            assertQuery("SELECT sym, val, rn FROM lv_starts ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\trn\n" +
                    "aaa\t1\t1\n" +
                    "aaa\t5\t2\n");
            assertQuery("SELECT sym, val, rn FROM lv_ends ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\trn\n" +
                    "aaa\t1\t1\n" +
                    "aaa\t5\t2\n");
            assertQuery("SELECT sym, val, rn FROM lv_ilike ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\trn\n" +
                    "aaa\t1\t1\n" +
                    "xax\t3\t2\n" +
                    "aaa\t5\t3\n");
            assertQuery("SELECT sym, val, rn FROM lv_wildcard ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\trn\n" +
                    "xax\t3\t1\n");
            assertQuery("SELECT sym, val, rn FROM lv_regex ORDER BY ts").noLeakCheck().expectSize().returns("sym\tval\trn\n" +
                    "aaa\t1\t1\n" +
                    "xax\t3\t2\n" +
                    "aaa\t5\t3\n");
            // Every view stayed healthy: the NPE shapes used to land here as INVALID.
            assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            execute("DROP LIVE VIEW lv_contains");
            execute("DROP LIVE VIEW lv_starts");
            execute("DROP LIVE VIEW lv_ends");
            execute("DROP LIVE VIEW lv_ilike");
            execute("DROP LIVE VIEW lv_wildcard");
            execute("DROP LIVE VIEW lv_regex");
        });
    }

    @Test
    public void testRefreshWithSymbolBindingInLagDefault() throws Exception {
        // The third lag argument can hold symbol bindings: sym = 'ccc' compiles to a
        // function that resolves the literal to a symbol key when it binds and then
        // compares raw keys per row. The first refresh cycle is a bootstrap that fully
        // initializes every function; each later cycle re-binds the window function's
        // expressions through initPartitionBy(), and the partitioned lag function
        // forwards that rebind to its default. 'ccc' is absent from the dictionary the
        // bootstrap binds against, so the equality caches a not-found verdict for the
        // literal; the second cycle introduces 'ccc', and only the per-cycle rebind
        // lets its first row resolve sym = 'ccc' to true and pick the matching CASE
        // arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DECIMAL(18,2), ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, price, ts, " +
                    "lag(price, 1, CASE WHEN sym = 'ccc' THEN 111.11::decimal(18, 2) ELSE 222.22::decimal(18, 2) END) " +
                    "OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS lg FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1 (bootstrap): 'ccc' is not in the dictionary yet, so the
                // equality binds a not-found verdict for the literal. Both rows open
                // new lag partitions and take the ELSE arm.
                execute("INSERT INTO base (sym, price, ts) VALUES " +
                        "('aaa', 1.00m, '2026-01-01T00:00:00.000000Z'), " +
                        "('bbb', 2.00m, '2026-01-01T00:01:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Advance past FLUSH EVERY so cycle 2 is not rate-limited.
                setCurrentMicros(2_000_000L);

                // Cycle 2: 'ccc' first appears here and opens a new lag partition, so
                // the refresh evaluates the CASE default for it against this cycle's
                // symbol view.
                execute("INSERT INTO base (sym, price, ts) VALUES " +
                        "('ccc', 3.00m, '2026-01-01T00:02:00.000000Z'), " +
                        "('bbb', 4.00m, '2026-01-01T00:03:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            // A refresh fault would self-heal into a full recompute whose complete
            // re-init resolves 'ccc' regardless of the per-cycle rebind; a clean run
            // pins the incremental path as the one that produced these rows.
            assertNoRefreshFaults("lv");

            // Each partition's first row takes the CASE default resolved for its own
            // row; every later row takes the previous value of the same partition.
            assertQuery("SELECT sym, price, lg FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tlg
                    aaa\t1.00\t222.22
                    bbb\t2.00\t222.22
                    ccc\t3.00\t111.11
                    bbb\t4.00\t2.00
                    """);
            assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectWhereOnDesignatedTimestamp() throws Exception {
        // C1 regression (interval half). A WHERE on the designated timestamp compiles into an
        // interval scan whose predicate lives in the frame cursor, not a residual filter Function,
        // so the incremental refresh path never sees it and every base row would slip through. There
        // is no residual-filter analogue to suppress it (unlike the indexed-symbol case), so CREATE
        // rejects it outright rather than silently building a view that ignores the filter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                        "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE ts > '2026-01-01T00:00:00.000000Z'");
                // Should not reach here; drop defensively so a spurious success does not
                // leave a view that trips a later assertion on the same name.
                execute("DROP LIVE VIEW lv");
                Assert.fail("expected reject for a WHERE on the designated timestamp");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "live view select cannot filter on the designated timestamp yet")
                );
            }
            Assert.assertNull("no view should survive the designated-timestamp reject",
                    engine.getLiveViewRegistry().getViewInstance("lv"));
        });
    }

    @Test
    public void testMultipleRefreshBatchesAccumulateState() throws Exception {
        assertMemoryLeak(() -> {
            // Pin a deterministic clock so the FLUSH EVERY rate-limit (1s) does
            // not coalesce batch 2 into batch 1 when both run in the same millisecond.
            setCurrentMicros(0);
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: rows 1, 2.
                execute("INSERT INTO base (val, ts) VALUES " +
                        "(10, '2026-01-01T00:00:00.000000Z'), " +
                        "(20, '2026-01-01T00:01:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Advance past FLUSH EVERY so batch 2's refresh is not rate-limited.
                setCurrentMicros(2_000_000L);

                // Batch 2: rows 3, 4 — rn must continue from 3, not restart at 1.
                execute("INSERT INTO base (val, ts) VALUES " +
                        "(30, '2026-01-01T00:02:00.000000Z'), " +
                        "(40, '2026-01-01T00:03:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertQuery("SELECT val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("val\tts\trn\n" +
                    "10\t2026-01-01T00:00:00.000000Z\t1\n" +
                    "20\t2026-01-01T00:01:00.000000Z\t2\n" +
                    "30\t2026-01-01T00:02:00.000000Z\t3\n" +
                    "40\t2026-01-01T00:03:00.000000Z\t4\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBaseTableAddColumnRecompilesFactory() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (val, ts) VALUES " +
                        "(10, '2026-01-01T00:00:00.000000Z'), " +
                        "(20, '2026-01-01T00:02:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // ADD COLUMN bumps the base metadata version without invalidating the
                // view (the new column is unreferenced). The cached factory is stale.
                execute("ALTER TABLE base ADD COLUMN extra DOUBLE");
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                // Out-of-order row: routes the refresh through the O3 replay, which
                // scans the base through the cached factory's page frames. The version
                // check must reject the stale factory and the recovery recompile +
                // recompute must produce the full, correct output.
                execute("INSERT INTO base (val, ts) VALUES (15, '2026-01-01T00:01:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertQuery("SELECT val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("val\tts\trn\n" +
                    "10\t2026-01-01T00:00:00.000000Z\t1\n" +
                    "15\t2026-01-01T00:01:00.000000Z\t2\n" +
                    "20\t2026-01-01T00:02:00.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBaseTableDropUnreferencedColumnRecompilesFactory() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            // "pad" sits BEFORE the referenced columns, so dropping it shifts the
            // reader indices of val and ts. The cached compiled factory still maps
            // the old layout; without the metadata-version check the O3 replay would
            // read the wrong columns with the wrong strides (garbage timestamps at
            // best, an out-of-bounds mmap read at worst).
            execute("CREATE TABLE base (pad SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (pad, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:00.000000Z'), " +
                        "('b', 20, '2026-01-01T00:02:00.000000Z'), " +
                        "('c', 30, '2026-01-01T00:03:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // The view does not reference pad, so the drop keeps it valid by
                // design - but the base metadata version moves past the factory's.
                execute("ALTER TABLE base DROP COLUMN pad");
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                // Out-of-order row: routes the refresh through the O3 replay's
                // page-frame scan over the stale factory. The recovery must
                // recompile and recompute the whole view against the new layout.
                execute("INSERT INTO base (val, ts) VALUES (15, '2026-01-01T00:01:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(4_000_000L);
                // Refresh must have resumed on the recompiled factory: a further
                // in-order row keeps the row_number sequence going.
                execute("INSERT INTO base (val, ts) VALUES (40, '2026-01-01T00:04:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertQuery("SELECT val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("val\tts\trn\n" +
                    "10\t2026-01-01T00:00:00.000000Z\t1\n" +
                    "15\t2026-01-01T00:01:00.000000Z\t2\n" +
                    "20\t2026-01-01T00:02:00.000000Z\t3\n" +
                    "30\t2026-01-01T00:03:00.000000Z\t4\n" +
                    "40\t2026-01-01T00:04:00.000000Z\t5\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshMapsOnlyProjectedWalColumns() throws Exception {
        // The incremental raw-WAL drain rebinds WalSegmentPageFrameCursor once per base
        // commit, and every rebind runs WalReader.loadColumnAt for the columns it maps.
        // That is not cheap: MemoryCMRImpl.of() munmaps, closes the fd, re-opens and
        // re-mmaps rather than remapping in place. Mapping the whole base schema therefore
        // charged a narrow view four syscalls per BASE column per commit - doubled for
        // variable-width ones - so a wide base table made every dependent view pay for
        // columns it never reads. The reader now takes the cursor's projection and leaves
        // the rest on NullMemoryCMR.
        //
        // Counting opens rather than timing anything keeps this deterministic. The counter
        // is armed only around the refresh drive: ApplyWal2TableJob legitimately reads every
        // column of the segment, so an always-on counter would measure the WAL apply too.
        final AtomicBoolean counting = new AtomicBoolean();
        final AtomicInteger unprojectedOpens = new AtomicInteger();
        final AtomicInteger projectedOpens = new AtomicInteger();
        // Filled in after CREATE TABLE. Both counters key on it because the view's OWN
        // output table also has a "val" column, and driveRefreshToQuiescence drains the WAL
        // queue inside the counting window - so applying the view's WAL would otherwise
        // satisfy the projected-column sanity check without the base segment being read
        // at all, leaving the whole assertion vacuous.
        final String[] baseDir = new String[1];
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // "wal" in the path restricts this to WAL segment column files: the same
                // column names also exist under the applied table directory, which a
                // from-base recompute would read.
                if (counting.get()
                        && baseDir[0] != null
                        && Utf8s.containsAscii(name, baseDir[0])
                        && Utf8s.containsAscii(name, "wal")) {
                    if (Utf8s.endsWithAscii(name, "pad9.d")) {
                        unprojectedOpens.incrementAndGet();
                    } else if (Utf8s.endsWithAscii(name, "val.d")) {
                        projectedOpens.incrementAndGet();
                    }
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (" +
                    "val INT, pad1 INT, pad2 INT, pad3 INT, pad4 INT, pad5 INT, " +
                    "pad6 INT, pad7 INT, pad8 INT, pad9 INT, ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR WAL");
            baseDir[0] = engine.verifyTableName("base").getDirName();
            // The view projects val and ts only; pad1..pad9 exist solely to be skipped.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");

                // Several commits into one segment: the per-commit rebind is exactly what
                // used to remap every base column, so more than one commit is the point.
                for (int i = 0; i < 4; i++) {
                    execute("INSERT INTO base (val, pad9, ts) VALUES " +
                            "(" + i + ", " + i + ", '2026-01-01T00:0" + i + ":00.000000Z')");
                }
                drainWalQueue();

                counting.set(true);
                try {
                    driveRefreshToQuiescence(job);
                } finally {
                    counting.set(false);
                }
            }
            drainWalQueue();

            // The drain must have read the segment through the projected column, otherwise
            // the zero below would be vacuous.
            Assert.assertTrue(
                    "the refresh must have mapped the projected column",
                    projectedOpens.get() > 0
            );
            Assert.assertEquals(
                    "an unprojected base column must never be mapped by the incremental refresh",
                    0,
                    unprojectedOpens.get()
            );
            // A refresh fault self-heals into a from-base recompute, which reads the applied
            // table rather than the segment and would hide a per-commit remap.
            assertNoRefreshFaults("lv");

            assertQuery("SELECT val, rn FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("val\trn\n" +
                    "0\t1\n" +
                    "1\t2\n" +
                    "2\t3\n" +
                    "3\t4\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshOnEmptyBaseProducesNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testExplainLiveViewQueryShowsLiveViewPlan() throws Exception {
        // Reading from a live view at SELECT time is a plain forward scan of the
        // LV's own materialized table, wrapped in a thin LiveView node - there is
        // no in-memory-tier merge node in the plan. Lead routing happens at cursor
        // iteration time (disk below the seam, the in-mem slot - overlap plus the
        // un-flushed lead - above it); the LiveView node's "inMemory" attribute
        // surfaces whether the read's static shape permits that routing. The
        // window function that defines the view runs only in the refresh job,
        // never at read time, so it is absent from the read plan too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // A SYMBOL output column is routable: the refresh worker stores
            // LV-table-space symbol ids the disk reader resolves on read, and the
            // rest of the schema is fixed-width on a forward, ts-bearing scan, so
            // inMemory is true here.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, price, ts, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            assertQuery("SELECT * FROM lv").noLeakCheck().assertsPlan("LiveView\n" +
                    "  view: lv\n" +
                    "  inMemory: true\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: lv\n");
            // A fixed-width, timestamp-bearing view on a forward scan also
            // permits lead routing: inMemory is true.
            execute("CREATE LIVE VIEW lv_fixed FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT price, ts, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            assertQuery("SELECT * FROM lv_fixed").noLeakCheck().assertsPlan("LiveView\n" +
                    "  view: lv_fixed\n" +
                    "  inMemory: true\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: lv_fixed\n");
            // A timestamp-pruned projection cannot seam (the base scan drops the
            // designated timestamp, leaving timestampColumnIndex < 0), but it routes
            // lead-only, which needs no timestamp to cut on - so inMemory is true here too.
            assertQuery("SELECT price, rn FROM lv_fixed").noLeakCheck().assertsPlan("LiveView\n" +
                    "  view: lv_fixed\n" +
                    "  inMemory: true\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: lv_fixed\n");
            execute("DROP LIVE VIEW lv");
            execute("DROP LIVE VIEW lv_fixed");
        });
    }

    @Test
    public void testOrderByTsDescElidesRedundantSort() throws Exception {
        // The LV factory delegates getScanDirection() to its base. An LV whose
        // base is a backward scan (ORDER BY ts DESC pushed down) therefore reports
        // BACKWARD, so the optimizer recognizes the order is already satisfied and
        // does not wrap the read in a redundant Sort node.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, price, ts, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            // A backward scan cannot seam - that split assumes ascending disk rows - but it
            // still routes lead-only (disk in full, plus the reversed lead), so inMemory
            // stays true. The elided sort is what this test is about and is independent of
            // routing: it turns on getScanDirection() alone.
            assertQuery("SELECT * FROM lv ORDER BY ts DESC").noLeakCheck().assertsPlan("LiveView\n" +
                    "  view: lv\n" +
                    "  inMemory: true\n" +
                    "    PageFrame\n" +
                    "        Row backward scan\n" +
                    "        Frame backward scan on: lv\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFilteredReadsRunThroughPageFrames() throws Exception {
        // Every filtered LV read reaches the parallel + JIT filter with LIMIT pushdown -
        // the same execution the identical read over a plain table gets - whether or not
        // it routes through the tier. The wrapper used to report the
        // supportsPageFrameCursor() default of false, so a filtered LV read fell back to
        // a single-threaded, interpreted Filter that could not stop early on the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, price, ts, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            // ORDER BY ts DESC is pushed into the base as a backward scan. The frame path
            // serves that disk-only - its seam cut takes the disk band by row count, which
            // a descending frame stream would cut at the wrong end - so this read gets the
            // base scan's frames unchanged. inMemory still reports true: the attribute is
            // the read SHAPE's capability, and the same query without the filter takes the
            // record path and does route (lead-only). This is the widest gap between
            // "routable" and "routed" the flag carries.
            assertQuery("SELECT * FROM lv WHERE sym = 'EURUSD' ORDER BY ts DESC LIMIT 10")
                    .noLeakCheck()
                    .assertsPlanContaining(
                            "Async",
                            "Filter",
                            "limit: 10",
                            "filter: sym='EURUSD'",
                            "LiveView",
                            "inMemory: true",
                            "Row backward scan"
                    );
            // A timestamp-pruned aggregate reaches the parallel filter as well, and this
            // one DOES route: the frame path's cut is by row count, so it never needed the
            // designated timestamp the projection drops.
            assertQuery("SELECT count() FROM lv WHERE sym = 'EURUSD'")
                    .noLeakCheck()
                    .assertsPlanContaining("Async", "Filter", "LiveView", "inMemory: true");
            // A forward, full-schema read routes through the tier AND reaches the
            // parallel filter, which runs over the tier's own frame. This arm asserted
            // notContaining("Async") while the two were exclusive: page frames came from
            // the base scan alone, so a routable read had to give them up to keep the
            // un-flushed lead. Fresh or fast, never both - and the filtered scan over
            // recent data, the read that most wants freshness, was the one the fork sent
            // to disk.
            assertQuery("SELECT * FROM lv WHERE sym = 'EURUSD'")
                    .noLeakCheck()
                    .assertsPlanContaining("Async", "Filter", "filter: sym='EURUSD'", "LiveView", "inMemory: true");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testShowColumnsReflectsLiveViewSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, price, ts, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            // upsertKey is false on every column. A live view created while
            // cairo.live.view.checkpoint.repair.sparse.publication.enabled is true carries the
            // designated timestamp and the projected partition key as dedup keys in its own
            // _meta, the identity a sparse repair publication upserts on, but its forward
            // commits keep rows that share them, so SHOW COLUMNS does not report them.
            assertQuery("SHOW COLUMNS FROM lv").noLeakCheck().noRandomAccess().returns("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\n" +
                    "sym\tSYMBOL\tfalse\t0\ttrue\t128\t0\tfalse\tfalse\t\t\n" +
                    "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t\n" +
                    "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n");
            // A live view is a physical WAL table that owns its symbol maps, so SHOW COLUMNS
            // opens a reader on the LV table itself and reports the real symbol table size.
            // A plain VIEW has no reader and always reports 0, so a non-zero size here proves
            // the LV took the reader path: 2 distinct symbols plus the null slot.
            execute("INSERT INTO base (sym, price, ts) VALUES " +
                    "('aaa', 1.0, '2026-01-01T00:00:00.000000Z'), " +
                    "('bbb', 2.0, '2026-01-01T00:01:00.000000Z'), " +
                    "('aaa', 3.0, '2026-01-01T00:02:00.000000Z'), " +
                    "(NULL, 4.0, '2026-01-01T00:03:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertQuery("SHOW COLUMNS FROM lv").noLeakCheck().noRandomAccess().returns("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\n" +
                    "sym\tSYMBOL\tfalse\t0\ttrue\t128\t3\tfalse\tfalse\t\t\n" +
                    "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t\n" +
                    "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    // A ZERO_PASS window function that does not support snapshots. No such GA function
    // exists, so this stub is the only way to reach (and pin) the supportsCheckpointState reject.
    private static final class NonSnapshotWindowFunction extends BaseWindowFunction {
        NonSnapshotWindowFunction() {
            super(null);
        }

        @Override
        public String getName() {
            return "test_no_snapshot";
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }
    }

    private static final class SnapshotWindowFunctionWithoutMetadata extends BaseWindowFunction {
        SnapshotWindowFunctionWithoutMetadata() {
            super(null);
        }

        @Override
        public String getName() {
            return "test_no_metadata";
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }
}
