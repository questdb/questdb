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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

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

    private void assertMutationRejected(String sql, String expectedMessageFragment) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym = 'bbb'");

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

    @Test
    public void testCreateLiveViewIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            // IF NOT EXISTS should succeed when the view already exists.
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewWithParenthesizedSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // A parenthesized SELECT must capture only the balanced inner query;
            // a stray leading '(' used to make the stored SQL fail to recompile.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS (SELECT val, ts, row_number() OVER () AS rn FROM base)");
            assertQuery("SELECT view_sql FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .noCircuitBreakerCheck() // catalogue function over the in-memory registry; no per-row checks
                    .returns("view_sql\nSELECT val, ts, row_number() OVER () AS rn FROM base\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateWritesLiveViewDefinitionToSequencerDir() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");

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
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

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
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

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
                    "SELECT ts, lo_sym, hi_sym, c1, row_number() OVER () AS rn FROM base " +
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
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
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
    public void testTablesReportsLiveView() throws Exception {
        // Locks the tables() discriminator asymmetry (documented in TablesFunctionFactory):
        // a live view is discoverable ONLY via table_type='L'. The matView BOOLEAN is
        // mat-view-only (false here) and there is deliberately no liveView BOOLEAN -
        // materialized views carry both matView=true and table_type='M', but adding a
        // symmetric liveView column would renumber every position-based tables() consumer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertQuery("SELECT table_type FROM tables() WHERE table_name = 'lv'").noLeakCheck().noRandomAccess().returns("table_type\nL\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInformationSchemaTablesShowsLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
                "view name expected, got table or materialized view name"
        );
    }

    @Test
    public void testRejectDropMaterializedViewOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP MATERIALIZED VIEW lv",
                "materialized view name expected, got table or view name"
        );
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
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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
    public void testRejectUnpartitionedAggregateWindowFunction() throws Exception {
        // An un-partitioned aggregate window (no PARTITION BY) is ZERO_PASS but has no
        // partition Map to snapshot, so it is not live-view-eligible and stays rejected
        // by nature - unlike the per-type migration train, this shape is never migratable.
        // It clears the pass-count check and hits the supportsSnapshot() reject in
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
                        "SELECT base1.val, base1.ts, row_number() OVER () AS rn FROM base1 " +
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
                    "SELECT val, ts, row_number() OVER () AS rn FROM base WHERE val > 5");
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
    public void testRefreshWithSymbolFilterUnderJitDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        assertSymbolEqualityFilterRefreshes();
    }

    @Test
    public void testRefreshWithSymbolFilterUnderJitEnabled() throws Exception {
        // CompiledFilterIRSerializer.serializeSymbolConstant has a live-view-specific gate: for an
        // ordinary compile it resolves the symbol constant against the base table's dictionary and
        // bakes the resulting int key into the filter as an immediate, but for a live view compile it
        // must not, because the keys in a raw WAL segment live in the segment's own space (clean
        // dictionary keys, then the per-txn diff band - see WalSegmentPageFrameCursor.WalSymbolTable),
        // not the base's. It forces the deferred bind-variable path instead.
        //
        // Be precise about what these two tests pin. No live view test varied the JIT mode at all, so
        // a symbol-filtered view's refresh was only ever exercised under whatever the default happened
        // to be; these drive it under both and require the same rows out of each. They do NOT cover
        // the gate itself, and nothing currently can: every refresh path in LiveViewRefreshJob applies
        // the Java residual filter Function (filterFactory.getFilter()) and never the native compiled
        // filter, so the JIT-compiled predicate does not execute during an incremental refresh.
        // Removing the gate leaves the whole live view suite green. Keep it - it is what would make
        // the compiled filter safe for the refresh path to use - but it is defensive rather than
        // load-bearing, and no test can honestly claim to protect it until the refresh path actually
        // runs the compiled filter.
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_ENABLED));
        assertSymbolEqualityFilterRefreshes();
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
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym = 'a'");
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
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym LIKE '%a%'");
            execute("CREATE LIVE VIEW lv_starts FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym LIKE 'a%'");
            execute("CREATE LIVE VIEW lv_ends FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym LIKE '%a'");
            execute("CREATE LIVE VIEW lv_ilike FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym ILIKE '%A%'");
            execute("CREATE LIVE VIEW lv_wildcard FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym LIKE 'x_x'");
            execute("CREATE LIVE VIEW lv_regex FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base WHERE sym ~ 'a'");

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
                        "SELECT val, ts, row_number() OVER () AS rn FROM base WHERE ts > '2026-01-01T00:00:00.000000Z'");
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
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
    public void testRefreshOnEmptyBaseProducesNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
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
            assertQuery("SHOW COLUMNS FROM lv").noLeakCheck().noRandomAccess().returns("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\n" +
                    "sym\tSYMBOL\tfalse\t0\ttrue\t128\t0\tfalse\tfalse\t\t\n" +
                    "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t\n" +
                    "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    // A ZERO_PASS window function that does not support snapshots. No such GA function
    // exists, so this stub is the only way to reach (and pin) the supportsSnapshot reject.
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
}
