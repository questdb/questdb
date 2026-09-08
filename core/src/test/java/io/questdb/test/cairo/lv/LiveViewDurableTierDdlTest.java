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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionRemovalEvents;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRetentionMarker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.mp.Job;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Grammar and compilation coverage for the live view durable-tier DDL: the {@code TTL} clause on
 * {@code CREATE LIVE VIEW}, and the {@code SET TTL} / {@code DROP PARTITION} / {@code CONVERT
 * PARTITION} verbs of {@code ALTER LIVE VIEW}.
 * <p>
 * The durable tier is the WAL table that backs the view, so all three ALTER verbs reuse the
 * {@code ALTER TABLE} parsers verbatim and are sequenced into the view's own WAL like any other
 * non-structural ALTER. What this class asserts is that each shape compiles, authorizes, sequences
 * and round-trips through the catalogue, and what an applied removal does to the view's runtime:
 * the removal events the writer records, the in-memory tier's consistency, the lifetime row
 * counter, the checkpoint-timeline retention it publishes, the durable retention marker, and the
 * in-memory tier rebuild over a Parquet partition the conversion left inside the view's
 * {@code IN MEMORY} window. Seed recovery and replica propagation belong to the later stages.
 */
public class LiveViewDurableTierDdlTest extends AbstractLiveViewTest {

    // The live view's lower bound is the CREATE wall-clock moment and the refresh path drops rows
    // below it, so pin the clock under the test data. See LiveViewTest.pinClockBelowTestData.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAlterLiveViewConvertPartitionCompilesAndSequences() throws Exception {
        // CONVERT PARTITION TO PARQUET / TO NATIVE, both selectors, plus the Parquet-only
        // WITH (...) options. Each shape must reach the view's WAL as its own transaction.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1), ('1970-01-03T00:00:00.000000Z', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                final TableToken lvToken = engine.verifyTableName("lv");
                long seqTxn = engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn();

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '1970-01-01' WITH (BLOOM_FILTER_COLUMNS = 'x')");
                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO NATIVE LIST '1970-01-01'");
                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO NATIVE WHERE ts < '1970-01-02'");

                Assert.assertEquals(
                        "each CONVERT PARTITION must sequence exactly one live view transaction",
                        seqTxn + 5,
                        engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn()
                );

                driveLiveViewWalApply(job);
                Assert.assertFalse(
                        "applying CONVERT PARTITION must not suspend the live view",
                        engine.getTableSequencerAPI().isSuspended(lvToken)
                );
            }
        });
    }

    @Test
    public void testAlterLiveViewDropPartitionCompilesAndSequences() throws Exception {
        // DROP PARTITION accepts both selectors of ALTER TABLE and sequences one transaction each.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1), ('1970-01-03T00:00:00.000000Z', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                final TableToken lvToken = engine.verifyTableName("lv");
                final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn();

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                execute("ALTER LIVE VIEW lv DROP PARTITION WHERE ts < '1970-01-02'");

                Assert.assertEquals(
                        "each DROP PARTITION must sequence exactly one live view transaction",
                        seqTxn + 2,
                        engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn()
                );

                // Applying them must not suspend the view. The second statement matches nothing by
                // apply time - the first already removed 1970-01-01 - which the partition helpers
                // report as a recoverable CairoException: ApplyWal2TableJob marks the seqTxn
                // committed and moves on.
                driveLiveViewWalApply(job);
                Assert.assertFalse(
                        "applying DROP PARTITION must not suspend the live view",
                        engine.getTableSequencerAPI().isSuspended(lvToken)
                );
            }
        });
    }

    @Test
    public void testAlterLiveViewRejectedShapes() throws Exception {
        // Everything outside SET TTL / DROP PARTITION / CONVERT PARTITION / RESUME WAL / SUSPEND WAL
        // stays rejected: a live view's schema is a function of its SELECT, and the verbs that
        // rewrite storage in place race the refresh worker's inline apply.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            // ALTER TABLE never reaches a live view at all.
            assertRejected("ALTER TABLE lv SET TTL 1 DAY", "cannot modify live view");

            // Verbs the live view grammar does not have.
            assertRejected("ALTER LIVE VIEW lv RENAME TO lv2", "'set', 'drop', 'convert', 'resume' or 'suspend' expected");
            assertRejected("ALTER LIVE VIEW lv ADD COLUMN y INT", "'set', 'drop', 'convert', 'resume' or 'suspend' expected");
            assertRejected("ALTER LIVE VIEW lv DETACH PARTITION LIST '1970-01-01'", "'set', 'drop', 'convert', 'resume' or 'suspend' expected");
            assertRejected("ALTER LIVE VIEW lv ATTACH PARTITION LIST '1970-01-01'", "'set', 'drop', 'convert', 'resume' or 'suspend' expected");
            assertRejected("ALTER LIVE VIEW lv SQUASH PARTITIONS", "'set', 'drop', 'convert', 'resume' or 'suspend' expected");
            assertRejected("ALTER LIVE VIEW lv DEDUP DISABLE", "'set', 'drop', 'convert', 'resume' or 'suspend' expected");
            assertRejected("ALTER LIVE VIEW lv ALTER COLUMN x TYPE LONG", "'set', 'drop', 'convert', 'resume' or 'suspend' expected");

            // SET accepts TTL and nothing else.
            assertRejected("ALTER LIVE VIEW lv SET PARAM maxUncommittedRows = 1", "'ttl' expected");
            assertRejected("ALTER LIVE VIEW lv SET FORMAT PARQUET", "'ttl' expected");
            assertRejected("ALTER LIVE VIEW lv SET TYPE BYPASS WAL", "'ttl' expected");

            // DROP is DROP PARTITION only; the column form has no meaning for a live view.
            assertRejected("ALTER LIVE VIEW lv DROP COLUMN x", "'partition' expected");

            // FORCE DROP PARTITION bypasses the WAL and writes through a directly acquired
            // TableWriter, which the refresh worker's inline apply owns.
            assertRejected(
                    "ALTER LIVE VIEW lv FORCE DROP PARTITION LIST '1970-01-01'",
                    "FORCE DROP PARTITION is not supported on live views"
            );

            // The shared partition parsers report their own errors unchanged.
            assertRejected("ALTER LIVE VIEW lv DROP PARTITION LIST 'bogus'", "'yyyy-MM-dd' expected");
            assertRejected("ALTER LIVE VIEW lv DROP PARTITION WHERE ts < '1969-01-01'", "no partitions matched WHERE clause");
            assertRejected("ALTER LIVE VIEW lv CONVERT PARTITION TO NATIVE LIST '1970-01-01' WITH (BLOOM_FILTER_COLUMNS = 'x')", "',' expected");
            assertRejected("ALTER LIVE VIEW lv SET TTL 1 HOUR", "TTL value must be an integer multiple of the partition size");
        });
    }

    @Test
    public void testAlterLiveViewSetTtl() throws Exception {
        // SET TTL is sequenced into the view's WAL and applied by the refresh worker, after which
        // it is table metadata like any other TTL: tables() reports it and SHOW CREATE re-emits it.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertTtl(0);

                execute("ALTER LIVE VIEW lv SET TTL 4 WEEKS");
                driveLiveViewWalApply(job);
                assertTtl(28 * 24);
                // noLeakCheck: the leak-checking wrapper calls engine.clear(), which empties the
                // live view registry and would leave the refresh job below with nothing to drive.
                assertQuery("SELECT ttlValue, ttlUnit FROM tables() WHERE table_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("ttlValue\tttlUnit\n4\tWEEK\n");
                assertShowCreateContains(" PARTITION BY DAY TTL 4 WEEKS START FROM NOW");

                // Months-based TTL round-trips through the same field with a negative encoding.
                execute("ALTER LIVE VIEW lv SET TTL 1 YEAR");
                driveLiveViewWalApply(job);
                assertTtl(-12);
                assertShowCreateContains(" PARTITION BY DAY TTL 1 YEAR START FROM NOW");

                // Clearing needs a unit; the bare form is rejected by the shared parser.
                assertRejected("ALTER LIVE VIEW lv SET TTL 0", "missing unit");
                execute("ALTER LIVE VIEW lv SET TTL 0 HOURS");
                driveLiveViewWalApply(job);
                assertTtl(0);
                assertShowCreateContains(" PARTITION BY DAY START FROM NOW");

                // The <number><unit> shorthand works for setting and for clearing.
                execute("ALTER LIVE VIEW lv SET TTL 2d");
                driveLiveViewWalApply(job);
                assertTtl(48);
                execute("ALTER LIVE VIEW lv SET TTL 0h");
                driveLiveViewWalApply(job);
                assertTtl(0);
            }
        });
    }

    @Test
    public void testCreateLiveViewTtl() throws Exception {
        // The TTL clause is optional, may appear anywhere in the clause loop, and lands in the
        // view's _meta through LiveViewTableStructure.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 1s TTL 3 DAYS PARTITION BY DAY START FROM NOW AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1s PARTITION BY DAY START FROM NOW TTL 3 DAYS AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            // No explicit PARTITION BY: the scheme is inherited from the base table and the
            // granularity check runs in CairoEngine.createLiveView instead of the parser.
            execute("CREATE LIVE VIEW lv3 FLUSH EVERY 1s TTL 3 DAYS START FROM NOW AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");

            assertTtl("lv1", 72);
            assertTtl("lv2", 72);
            assertTtl("lv3", 72);
            assertQuery("SELECT table_name, ttlValue, ttlUnit FROM tables() WHERE table_name LIKE 'lv%' ORDER BY table_name")
                    .returns("""
                            table_name\tttlValue\tttlUnit
                            lv1\t3\tDAY
                            lv2\t3\tDAY
                            lv3\t3\tDAY
                            """);
            assertShowCreateContains("lv1", " PARTITION BY DAY TTL 3 DAYS START FROM NOW");
            assertShowCreateRoundTrips("lv1");
        });
    }

    @Test
    public void testCreateLiveViewTtlRejections() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String tail = " START FROM NOW AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)";

            assertRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s TTL 3 DAYS TTL 4 DAYS" + tail,
                    "live view TTL clause specified more than once"
            );
            // Explicit PARTITION BY: the parser validates granularity before the engine sees it.
            assertRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY DAY TTL 1 HOUR" + tail,
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY DAY TTL ".length(),
                    "TTL value must be an integer multiple of the partition size"
            );
            // Inherited PARTITION BY: the same check runs in CairoEngine.createLiveView, and points
            // at the same TTL value position.
            assertRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s TTL 1 HOUR" + tail,
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s TTL ".length(),
                    "TTL value must be an integer multiple of the partition size"
            );
            assertRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s TTL 3" + tail,
                    "invalid unit, expected 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)', but was 'START'"
            );
            // FLUSH EVERY still has to come first.
            assertRejected(
                    "CREATE LIVE VIEW lv TTL 3 DAYS FLUSH EVERY 1s" + tail,
                    "'flush every <duration>' expected"
            );
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testDropPartitionActivePartitionRejectedAtApplyTime() throws Exception {
        // The compile-time guard reads a TableReader snapshot the statement outlives, so it cannot
        // be the last word. Sequencing the DROP straight into the view's WAL - which is what a
        // replicated command or an older binary's statement amounts to - skips that guard and lets
        // the writer's own check answer. It must reject the active target as recoverable: the
        // transaction is marked applied and the view stays active rather than suspending.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1), ('1970-01-03T00:00:00.000000Z', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                final TableToken lvToken = engine.verifyTableName("lv");
                sequenceRawDropPartition(lvToken, "1970-01-03");
                driveLiveViewWalApply(job);

                Assert.assertFalse(
                        "an active-partition DROP must be tolerated, not suspend the live view",
                        engine.getTableSequencerAPI().isSuspended(lvToken)
                );
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-01
                                1970-01-03
                                """);
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-01T00:00:00.000000Z\t1\t1
                                1970-01-03T00:00:00.000000Z\t3\t1
                                """);

                // The view keeps applying afterwards: a tolerated failure is not a stall.
                execute("INSERT INTO base VALUES ('1970-01-04T00:00:00.000000Z', 4)");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            }
        });
    }

    @Test
    public void testDropPartitionAllowedOnceTargetIsNoLongerActive() throws Exception {
        // The writer decides from the partition set the removal would act on, not from what the
        // compiler saw, so a partition that was the frontier when it was first named becomes a
        // legal target as soon as a newer one exists. Note this shape does NOT exercise the
        // rejection: creating a newer partition is what makes the target droppable.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1), ('1970-01-03T00:00:00.000000Z', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertRejected(
                        "ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-03'",
                        "cannot drop the active partition of a live view [partition=1970-01-03T00:00:00.000000Z]"
                );

                // A newer partition takes over the frontier.
                execute("INSERT INTO base VALUES ('1970-01-05T00:00:00.000000Z', 5)");
                driveUntilDurableRowCount(job, 3);

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-03'");
                driveLiveViewWalApply(job);

                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("lv")));
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-01
                                1970-01-05
                                """);
            }
        });
    }

    @Test
    public void testDropPartitionRejectsActivePartitionAtCompileTime() throws Exception {
        // The newest partition is the durable frontier the refresh pipeline appends into and an
        // out-of-order repair rewrites, so DROP PARTITION must not name it - through either
        // selector, and whether or not the statement also names droppable partitions.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1), " +
                    "('1970-01-02T00:00:00.000000Z', 2), " +
                    "('1970-01-03T00:00:00.000000Z', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                final TableToken lvToken = engine.verifyTableName("lv");
                final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn();
                final String expected = "cannot drop the active partition of a live view [partition=1970-01-03T00:00:00.000000Z]";

                // LIST, naming the frontier. The error points at the partition name.
                assertRejected(
                        "ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-03'",
                        "ALTER LIVE VIEW lv DROP PARTITION LIST ".length(),
                        expected
                );
                // A split-partition name normalizes to the logical partition it belongs to, so it
                // cannot be used to reach the frontier through the back door.
                assertRejected("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-03T12'", expected);
                // Droppable partitions in the same list do not make the statement partly legal.
                assertRejected("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01', '1970-01-03'", expected);
                assertRejected("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-03', '1970-01-01'", expected);
                // WHERE, matching the frontier alone and matching it alongside older partitions.
                assertRejected("ALTER LIVE VIEW lv DROP PARTITION WHERE ts >= '1970-01-03'", expected);
                assertRejected("ALTER LIVE VIEW lv DROP PARTITION WHERE ts >= '1970-01-02'", expected);

                Assert.assertEquals(
                        "a rejected DROP PARTITION must not reach the live view's WAL",
                        seqTxn,
                        engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn()
                );

                // Everything below the frontier is droppable through either selector. The two
                // statements name disjoint partitions, so neither depends on the other's outcome.
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                execute("ALTER LIVE VIEW lv DROP PARTITION WHERE ts >= '1970-01-02' AND ts < '1970-01-03'");
                driveLiveViewWalApply(job);
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-03
                                """);
            }

            // The guard is live-view only: a plain WAL table still drops its newest partition.
            execute("CREATE TABLE plain (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO plain VALUES ('1970-01-01T00:00:00.000000Z', 1), ('1970-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            execute("ALTER TABLE plain DROP PARTITION LIST '1970-01-03'");
            drainWalQueue();
            assertQuery("SELECT count() FROM plain").noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testDropPartitionOnIdleViewRebuildsTierAndReconcilesTimeline() throws Exception {
        // A DROP on a view with nothing to flush lands through the lagging scan's apply retry:
        // rows leave the durable tier without a base commit, the in-memory tier is rebuilt
        // from the surviving table so seam routing resumes, the lifetime counter drops by
        // exactly the removed rows, and the checkpoint timeline is reconciled in place: the
        // single root above the dropped day survives with its position lowered by the row
        // that went, the retention marker is cleared only once that generation is durable,
        // and a restart restores from the timeline rather than rebuilding - so the dropped
        // day stays dropped.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final AtomicLong generationAtMarkerClear = new AtomicLong(Numbers.LONG_NULL);
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.RETENTION_MARKER_FILE_NAME)) {
                    // The clear is ordered after the retention generation's commit, so the
                    // generation on disk at this moment must already be the corrected one.
                    generationAtMarkerClear.set(readGeneration(engine.verifyTableName("lv")));
                }
                return super.removeQuiet(name);
            }
        }, () -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1), " +
                    "('1970-01-02T00:00:00.000000Z', 2), " +
                    "('1970-01-03T00:00:00.000000Z', 3)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(3, instance.getLvRowsTotal());
                // One flush, one root: at the flush's frontier, counting all three rows.
                assertLadder(instance, ts("1970-01-03"), 3);
                final long generationBefore = readGeneration(lvToken);

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                driveLiveViewWalApply(job);

                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-02T00:00:00.000000Z\t2\t1
                                1970-01-03T00:00:00.000000Z\t3\t1
                                """);
                // The page-frame read path agrees with the record path.
                assertQuery("SELECT min(x) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("min\n2\n");
                assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
                Assert.assertFalse("the removal must be reconciled, not left pending", instance.hasPendingPartitionRemovals());
                Assert.assertEquals("the lifetime counter must drop by the removed rows", 2, instance.getLvRowsTotal());
                // The timeline survives: no root sat inside the dropped day, and the head above
                // it now reports the two rows the table holds below its boundary.
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(generationBefore + 1, readGeneration(lvToken));
                Assert.assertEquals(0, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-03"), 2);
                assertRetentionMarker(lvToken, false);
                Assert.assertEquals(
                        "the marker must be cleared only after the retention generation is published",
                        generationBefore + 1,
                        generationAtMarkerClear.get()
                );
                // The head is untouched, so the seal cadence keeps its baseline rather than
                // starting a fresh history.
                Assert.assertEquals(ts("1970-01-03"), instance.getHeadCheckpointMaxTs());
                // The tier was rebuilt from the surviving table: a fresh cursor seams again
                // instead of falling back to disk-only.
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);

                // The next flush seals on top of the corrected head, and the removal never
                // registers as an unexplained row-count mismatch.
                execute("INSERT INTO base VALUES ('1970-01-04T00:00:00.000000Z', 4)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(3, instance.getLvRowsTotal());
                assertLadder(instance, ts("1970-01-03"), 2, ts("1970-01-04"), 3);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            // A restart restores from the reconciled timeline: the dropped day does not come
            // back, and the view carries on from the restored head.
            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn
                            1970-01-02T00:00:00.000000Z\t2\t1
                            1970-01-03T00:00:00.000000Z\t3\t1
                            1970-01-04T00:00:00.000000Z\t4\t1
                            """);
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropInteriorPartitionRetiresItsRootAndKeepsTheAnchorsAroundIt() throws Exception {
        // One root per day, then the middle day goes. The retention retires exactly the root
        // whose boundary sat inside the dropped day, keeps the anchor below it at its position
        // and lowers the head above it by the one row that went - and a restart restores from
        // that ladder with the dropped day still gone.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                flushRow(job, "1970-01-02", 2, 2);
                flushRow(job, "1970-01-03", 3, 3);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(instance, ts("1970-01-01"), 1, ts("1970-01-02"), 2, ts("1970-01-03"), 3);

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-02'");
                driveLiveViewWalApply(job);

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-01T00:00:00.000000Z\t1\t1
                                1970-01-03T00:00:00.000000Z\t3\t1
                                """);
                Assert.assertEquals(2, instance.getLvRowsTotal());
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(1, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-01"), 1, ts("1970-01-03"), 2);
                assertRetentionMarker(lvToken, false);
                Assert.assertEquals(ts("1970-01-03"), instance.getHeadCheckpointMaxTs());
                assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-03T00:00:00.000000Z\t3\t1
                            """);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-04", 4, 3);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(instance, ts("1970-01-01"), 1, ts("1970-01-03"), 2, ts("1970-01-04"), 3);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDropDisjointPartitionsCorrectsEachSurvivorByItsOwnRemovedRows() throws Exception {
        // Two separated days go in one LIST. The roots inside them retire, the root in the
        // gap between them keeps its anchor and is lowered only by the day below it, and the
        // roots above both are lowered by both - one difference-array breakpoint per interval,
        // never a min/max envelope that would have taken the gap's root with it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int day = 1; day <= 5; day++) {
                    flushRow(job, "1970-01-0" + day, day, day);
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(
                        instance,
                        ts("1970-01-01"), 1,
                        ts("1970-01-02"), 2,
                        ts("1970-01-03"), 3,
                        ts("1970-01-04"), 4,
                        ts("1970-01-05"), 5
                );

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01', '1970-01-03'");
                driveLiveViewWalApply(job);

                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n4\n5\n");
                Assert.assertEquals(3, instance.getLvRowsTotal());
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(2, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-02"), 1, ts("1970-01-04"), 2, ts("1970-01-05"), 3);
                assertRetentionMarker(lvToken, false);
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n4\n5\n");
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropPartitionAboveLaggingHeadRetiresTimelineAtOnce() throws Exception {
        // The head can lag the table by several partitions on an idle view. A partition above
        // the head is not counted in its position, but a restart's replay from the base would
        // re-emit the removed rows and fail its row-count proof against the smaller table, so
        // no root can be trusted: the DDL disposes of the timeline immediately, without
        // waiting for a base commit, and the restart takes the documented applied-base rebuild.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 3);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES " +
                        "('1970-01-01T00:00:00.000000Z', 1), " +
                        "('1970-01-02T00:00:00.000000Z', 2), " +
                        "('1970-01-03T00:00:00.000000Z', 3)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                // Two more single-row flushes stay under the three-row cadence: the head
                // stays at 1970-01-03 while the table runs on to 1970-01-05.
                flushRow(job, "1970-01-04", 4, 4);
                flushRow(job, "1970-01-05", 5, 5);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(instance, ts("1970-01-03"), 3);
                Assert.assertEquals(ts("1970-01-03"), instance.getHeadCheckpointMaxTs());

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-04'");
                driveLiveViewWalApply(job);

                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n3\n5\n");
                Assert.assertEquals(4, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertTimelineExists(lvToken, false);
                assertRetentionMarker(lvToken, false);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
                assertNoRefreshFaults("lv");
            }

            // No trusted timeline is left, so the restart rebuilds from the applied base. The
            // base still holds 1970-01-04, so the DROP is undone - the documented semantics.
            restartAndAssertRebuiltFromAppliedBase("timeline is absent");
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n3\n4\n5\n");
            Assert.assertEquals(5, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropPartitionHoldingTheHeadRetiresTimeline() throws Exception {
        // The head's own boundary sits inside the dropped day, so its output is gone and no
        // older root can stand in for it: the timeline retires whole and a fresh history opens
        // at the next seal, at the corrected position.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 3);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES " +
                        "('1970-01-01T00:00:00.000000Z', 1), " +
                        "('1970-01-02T00:00:00.000000Z', 2), " +
                        "('1970-01-03T00:00:00.000000Z', 3)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                flushRow(job, "1970-01-04", 4, 4);
                flushRow(job, "1970-01-05", 5, 5);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(instance, ts("1970-01-03"), 3);

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-03'");
                driveLiveViewWalApply(job);

                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n4\n5\n");
                Assert.assertEquals(4, instance.getLvRowsTotal());
                assertTimelineExists(lvToken, false);
                assertRetentionMarker(lvToken, false);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());

                // The next flush is the fresh history's first seal, at the corrected position.
                flushRow(job, "1970-01-06", 6, 5);
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("1970-01-06"), 5);
                Assert.assertEquals(0, readRetiredCheckpointCount(lvToken));
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n4\n5\n6\n");
        });
    }

    @Test
    public void testRetentionPublicationFailureFallsBackToRetire() throws Exception {
        // The retention generation fails after its metadata segments are written and before
        // the superblock commits. The previous generation stays authoritative for that
        // instant, then the fallback retires the timeline whole - marker included - so no
        // root outlives the rows it counted, and the view carries on: the counter is
        // corrected, reads are right, and the next seal opens a fresh history.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                flushRow(job, "1970-01-02", 2, 2);
                flushRow(job, "1970-01-03", 3, 3);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                capture.start();
                try {
                    execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-02'");
                    driveLiveViewWalApply(job);
                    capture.waitFor("could not publish live view checkpoint retention, retiring the timeline [view=lv");
                } finally {
                    capture.stop();
                }
                job.setCheckpointTimelineTestFailureStage(0);

                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n3\n");
                Assert.assertEquals(2, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertTimelineExists(lvToken, false);
                assertRetentionMarker(lvToken, false);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

                flushRow(job, "1970-01-04", 4, 3);
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("1970-01-04"), 3);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testRetentionMarkerIsDurableBeforeRemovalAndForcesRebuildOnRestart() throws Exception {
        // The marker is what a restart trusts over a row count that can coincide. Two things
        // are pinned: the writer publishes it before the removal's commit, whichever job
        // drives the apply - here the global apply job, as on a node with refresh disabled,
        // which reconciles nothing and so leaves the marker and the stale timeline behind -
        // and a restart that finds it rebuilds from the applied base instead of restoring
        // from that timeline. The rebuild re-materialises the dropped period from the base,
        // which is the documented DROP PARTITION recovery semantics.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final AtomicInteger markerPublishes = new AtomicInteger();
        final AtomicBoolean partitionAttachedAtMarkerPublish = new AtomicBoolean();
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.RETENTION_MARKER_FILE_NAME)) {
                    markerPublishes.incrementAndGet();
                    // The publish is the last write before the removal's _txn commit, so a
                    // reader opened here must still see every partition attached.
                    try (TableReader reader = engine.getReader(engine.verifyTableName("lv"))) {
                        partitionAttachedAtMarkerPublish.set(reader.getPartitionCount() == 3);
                    }
                }
                return super.rename(from, to);
            }
        }, () -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1), " +
                    "('1970-01-02T00:00:00.000000Z', 2), " +
                    "('1970-01-03T00:00:00.000000Z', 3)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
            }
            assertTimelineExists(lvToken, true);

            execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
            final long dropSeqTxn = engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn();
            try (ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 1)) {
                applyJob.applyWalDirect(lvToken, Job.RUNNING_STATUS);
                final PartitionRemovalEvents events = applyJob.getCommittedRemovalEvents();
                Assert.assertEquals(1, events.size());
                Assert.assertEquals(dropSeqTxn, events.getSeqTxn(0));
                Assert.assertEquals(ts("1970-01-01"), events.getLo(0));
                Assert.assertEquals(ts("1970-01-02"), events.getHiExclusive(0));
                Assert.assertEquals(1, events.getRemovedRows(0));
                Assert.assertFalse(events.isTtl(0));
            }
            Assert.assertEquals(1, markerPublishes.get());
            Assert.assertTrue("the marker must be published before the removal commits", partitionAttachedAtMarkerPublish.get());
            assertRetentionMarker(lvToken, true);
            try (Path checkpointsDir = new Path()) {
                Assert.assertEquals(
                        dropSeqTxn,
                        LiveViewRetentionMarker.readSeqTxn(configuration, checkpointsDir(checkpointsDir, lvToken), lvToken.getTableId())
                );
            }
            // The global apply reconciles nothing: the timeline still counts the removed row.
            assertTimelineExists(lvToken, true);
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            // Restart: the registry is rebuilt from disk and the first cycle recovers.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                capture.waitFor("live view restart rebuilding from applied base [view=lv, cause=pending retention marker present");
            } finally {
                capture.stop();
            }
            assertRetentionMarker(lvToken, false);
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertTrue(instance.isCheckpointRestoreSucceeded());
            Assert.assertFalse(instance.hasPendingPartitionRemovals());
            // Recovery re-derived the view from START FROM, and the base still holds the
            // dropped day: the DROP is undone, exactly as the semantics say.
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-02T00:00:00.000000Z\t2\t1
                            1970-01-03T00:00:00.000000Z\t3\t1
                            """);
            Assert.assertEquals(3, instance.getLvRowsTotal());
            assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testRetentionMarkerWriteFailureFailsTheRemoval() throws Exception {
        // Fail closed: if the evidence that rows are about to go missing cannot be made
        // durable, the rows do not go missing. The apply reports the fault by suspending the
        // view, the partition stays attached, nothing claims a removal happened, and RESUME WAL
        // re-drives the DROP once the fault clears.
        final AtomicBoolean rejectMarkerPublish = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (rejectMarkerPublish.get() && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.RETENTION_MARKER_FILE_NAME)) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        }, () -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1), " +
                    "('1970-01-02T00:00:00.000000Z', 2), " +
                    "('1970-01-03T00:00:00.000000Z', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                final TableToken lvToken = engine.verifyTableName("lv");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                rejectMarkerPublish.set(true);
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                driveUntil(job, () -> engine.getTableSequencerAPI().isSuspended(lvToken), "the live view was not suspended");

                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-01
                                1970-01-02
                                1970-01-03
                                """);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
                assertRetentionMarker(lvToken, false);
                Assert.assertFalse("a removal that did not commit must not be reported", instance.hasPendingPartitionRemovals());
                Assert.assertEquals(3, instance.getLvRowsTotal());

                rejectMarkerPublish.set(false);
                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveLiveViewWalApply(job);
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                """);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                Assert.assertEquals(2, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
            }
        });
    }

    @Test
    public void testTtlEvictionInsideFlushReconcilesCounterAndRebuildsTier() throws Exception {
        // TTL eviction is the one removal that lands inside the flush's own commit, so the
        // writer txn advances by exactly one and the flush would otherwise re-stamp a slot
        // holding rows the table just lost. The flush must un-stamp it instead, take the
        // evicted rows off the lifetime counter, and seal the fresh history at the corrected
        // position - with no row-count mismatch recorded and the next cycle rebuilding the
        // tier from the surviving table.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1), ('1970-01-02T00:00:00.000000Z', 2)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 2);
                driveRefreshToQuiescence(job);
                final TableToken lvToken = engine.verifyTableName("lv");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                execute("ALTER LIVE VIEW lv SET TTL 1 DAY");
                driveLiveViewWalApply(job);
                // 1970-01-01's ceiling is 1970-01-02; a day past that is 1970-01-03, which the
                // table has not reached, so applying the TTL itself evicts nothing yet.
                assertQuery("SELECT count() FROM table_partitions('lv')").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                Assert.assertEquals(2, instance.getLvRowsTotal());

                // The flush that lands 1970-01-03 evicts 1970-01-01 inside its own commit. TTL
                // judges age by the smaller of the table's frontier and the wall clock, and the
                // clock was pinned below the data, so move it up to the new frontier first.
                setCurrentMicros(ts("1970-01-03T00:00:00.000000Z"));
                execute("INSERT INTO base VALUES ('1970-01-03T00:00:00.000000Z', 3)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 2 && reader.getPartitionCount() == 2 && reader.getMinTimestamp() == ts("1970-01-02");
                            }
                        },
                        "the flush never evicted 1970-01-01"
                );
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                """);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-02T00:00:00.000000Z\t2\t1
                                1970-01-03T00:00:00.000000Z\t3\t1
                                """);
                assertQuery("SELECT min(x), max(x) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("min\tmax\n2\t3\n");
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                Assert.assertEquals("the evicted row must leave the lifetime counter", 2, instance.getLvRowsTotal());
                assertRetentionMarker(lvToken, false);
                // The reconciliation and the seal happen in the same flush: the root the first
                // flush sealed at 1970-01-02 survives above the evicted day, lowered by the row
                // that went, and the seal appends 1970-01-03 on top of it. Nothing counted as a
                // mismatch, and nothing retired.
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(0, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-02"), 1, ts("1970-01-03"), 2);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");

                // The tier was un-stamped by the flush; the next cycle rebuilds it from the
                // surviving table and seam routing resumes.
                execute("INSERT INTO base VALUES ('1970-01-03T01:00:00.000000Z', 4)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
                Assert.assertEquals(3, instance.getLvRowsTotal());
                assertNoRefreshFaults("lv");
            }

            // A restart restores from the reconciled timeline, and the evicted day stays
            // evicted: recovery does not re-derive it.
            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn
                            1970-01-02T00:00:00.000000Z\t2\t1
                            1970-01-03T00:00:00.000000Z\t3\t1
                            1970-01-03T01:00:00.000000Z\t4\t1
                            """);
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testTtlEvictingTheOnlyRootRetiresAndResealsInTheSameFlush() throws Exception {
        // The view's one root sits in the day TTL evicts, so there is no survivor to correct:
        // the flush retires the timeline and its own seal, in the same cycle, opens a fresh
        // history at the corrected position - no mismatch recorded, marker gone.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(instance, ts("1970-01-01"), 1);

                execute("ALTER LIVE VIEW lv SET TTL 1 DAY");
                driveLiveViewWalApply(job);
                Assert.assertFalse(instance.hasPendingPartitionRemovals());

                setCurrentMicros(ts("1970-01-03T00:00:00.000000Z"));
                execute("INSERT INTO base VALUES ('1970-01-03T00:00:00.000000Z', 3)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 1 && reader.getMinTimestamp() == ts("1970-01-03");
                            }
                        },
                        "the flush never evicted 1970-01-01"
                );
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n3\n");
                Assert.assertEquals(1, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertTimelineExists(lvToken, true);
                Assert.assertEquals("a fresh history retires nothing", 0, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-03"), 1);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n3\n");
        });
    }

    @Test
    public void testConvertPartitionToParquetAndBackKeepsReadsExact() throws Exception {
        // A settled partition converted to Parquet is still read whole by both live view
        // cursor paths, and converting it back to native leaves the same rows behind. The
        // view's own IN MEMORY window is a second - one FLUSH EVERY - so the converted day
        // sits below it and the tier never has to read Parquet here; this is the read-path
        // half of the conversion, which the window rebuild below builds on.
        assertMemoryLeak(() -> {
            createParquetBaseAndView("1s");
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1, 'a', 'alpha'), " +
                    "('1970-01-02T00:00:00.000000Z', 2, 'b', 'beta'), " +
                    "('1970-01-03T00:00:00.000000Z', 3, 'a', 'gamma')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);
                assertParquetPartitionCount(1);
                assertParquetViewRows();
                assertNoRefreshFaults("lv");

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO NATIVE LIST '1970-01-01'");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);
                assertParquetPartitionCount(0);
                assertParquetViewRows();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testConvertWindowBoundaryPartitionToParquetRebuildsTier() throws Exception {
        // The partition holding the IN MEMORY window's lower edge goes to Parquet, so the
        // next tier rebuild has to read the window across a storage-format boundary: the
        // older half decodes, the newer half memcpy's, and the two make one dense
        // ts-ascending staging run. A Parquet partition publishes no per-column native
        // files, so before the decode branch the rebuild dereferenced a null column and
        // the refresh cycle faulted.
        //
        // What proves the rebuild landed is the routing mode: a cursor seams onto the slot
        // only while the slot is stamped with the disk reader's seqTxn, which the rebuild
        // is what re-stamps.
        assertMemoryLeak(() -> {
            createParquetBaseAndView("60m", "PARTITION BY HOUR");
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1, 'a', 'alpha'), " +
                    "('1970-01-01T01:00:00.000000Z', 2, 'b', 'beta'), " +
                    "('1970-01-01T02:00:00.000000Z', 3, 'a', 'gamma'), " +
                    "('1970-01-01T03:00:00.000000Z', 4, 'b', 'delta')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 4);
                driveRefreshToQuiescence(job);
                // IN MEMORY 60m against a 03:00 frontier puts the window's lower edge in the
                // 02:00 partition, so converting that one is what makes the rebuild read
                // Parquet - the 03:00 partition above it stays native.
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '1970-01-01T02'");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);

                assertParquetPartitionCount(1);
                assertNoRefreshFaults("lv");
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\tsym\tv\trn
                                1970-01-01T00:00:00.000000Z\t1\ta\talpha\t1
                                1970-01-01T01:00:00.000000Z\t2\tb\tbeta\t1
                                1970-01-01T02:00:00.000000Z\t3\ta\tgamma\t2
                                1970-01-01T03:00:00.000000Z\t4\tb\tdelta\t2
                                """);
                // The page-frame path agrees, and so does a SYMBOL read - the decode hands
                // back the same ids the memcpy branch copies out of the column file.
                assertQuery("SELECT sym, count() FROM lv ORDER BY 1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("sym\tcount\na\t2\nb\t2\n");

                // The view keeps refreshing on top of a window that spans both formats.
                execute("INSERT INTO base VALUES ('1970-01-01T04:00:00.000000Z', 5, 'a', 'epsilon')");
                driveUntilDurableRowCount(job, 5);
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, x, sym, v, rn FROM lv WHERE ts >= '1970-01-01T02'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("""
                                ts\tx\tsym\tv\trn
                                1970-01-01T02:00:00.000000Z\t3\ta\tgamma\t2
                                1970-01-01T03:00:00.000000Z\t4\tb\tdelta\t2
                                1970-01-01T04:00:00.000000Z\t5\ta\tepsilon\t3
                                """);

                // Back to native: the rebuild takes the memcpy branch again over the same window.
                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO NATIVE LIST '1970-01-01T02'");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);
                assertParquetPartitionCount(0);
                assertNoRefreshFaults("lv");
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
            }
        });
    }

    @Test
    public void testOutOfOrderRepairOverParquetPartitionSuspendsTheView() throws Exception {
        // KNOWN GAP, not a desired behaviour. This test pins it so that the day the writer
        // grows replace-mode support for Parquet the test fails and whoever did that work
        // replaces these assertions with the real expectation: the repair completes and the
        // view's rows match a recompute.
        //
        // A live view repairs an out-of-order base commit by publishing a REPLACE_RANGE over
        // its own table, and TableWriter.processO3Block refuses replace mode against a
        // Parquet partition outright ("commit replace mode is not supported for Parquet
        // partitions"). The refusal is a critical error, so the apply suspends the view.
        //
        // The range the head-miss replay publishes runs from the view's lower bound, so the
        // out-of-order row does not have to land inside the Parquet partition for the
        // replacement to cover it: the row this test inserts sits an hour ABOVE that
        // partition and the replacement reaches it anyway. Any out-of-order base commit
        // under a view holding any Parquet partition takes this path, which is why CONVERT
        // PARTITION TO PARQUET cannot be exposed on live views until it is fixed.
        assertMemoryLeak(() -> {
            createParquetBaseAndView("60m", "PARTITION BY HOUR");
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T01:00:00.000000Z', 1, 'a', 'alpha'), " +
                    "('1970-01-01T02:00:00.000000Z', 3, 'a', 'gamma'), " +
                    "('1970-01-01T03:00:00.000000Z', 4, 'b', 'delta')");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '1970-01-01T01'");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);
                assertParquetPartitionCount(1);

                // Out of order, and an hour ABOVE the Parquet partition.
                execute("INSERT INTO base VALUES ('1970-01-01T02:30:00.000000Z', 2, 'a', 'beta')");
                driveUntil(
                        job,
                        () -> engine.getTableSequencerAPI().isSuspended(lvToken),
                        "the repair's REPLACE_RANGE never reached the Parquet partition"
                );
                Assert.assertTrue(
                        "the refused replacement must suspend the view's own table",
                        engine.getTableSequencerAPI().isSuspended(lvToken)
                );
                // The repair never landed, so the out-of-order row is absent from the view
                // and the three rows the flush wrote are all it holds. Note that
                // live_views() still reports the view 'active' here: view_status does not
                // follow the durable tier's suspension.
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            }
        });
    }

    private static Path checkpointsDir(Path path, TableToken lvToken) {
        return path.of(configuration.getDbRoot())
                .concat(lvToken)
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static long readGeneration(TableToken lvToken) {
        try (Path path = new Path(); LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)) {
            store.of(checkpointsDir(path, lvToken));
            return store.isValid() ? store.getSuperblock().generation : Numbers.LONG_NULL;
        }
    }

    private static long readRetiredCheckpointCount(TableToken lvToken) {
        try (Path path = new Path(); LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)) {
            store.of(checkpointsDir(path, lvToken));
            Assert.assertTrue("the live view must hold a valid timeline", store.isValid());
            return store.getSuperblock().retiredCheckpointCount;
        }
    }

    private static LiveViewRecordCursorFactory unwrapLvFactory(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
        return (LiveViewRecordCursorFactory) f;
    }

    private static void assertTtl(String viewName, int expectedTtlHoursOrMonths) {
        final TableToken token = engine.verifyTableName(viewName);
        try (TableMetadata metadata = engine.getTableMetadata(token)) {
            Assert.assertEquals(
                    "live view '" + viewName + "' TTL",
                    expectedTtlHoursOrMonths,
                    metadata.getTtlHoursOrMonths()
            );
        }
    }

    private static void assertTtl(int expectedTtlHoursOrMonths) {
        assertTtl("lv", expectedTtlHoursOrMonths);
    }

    private void assertRejected(String sql, String expectedMessageFragment) {
        assertRejected(sql, -1, expectedMessageFragment);
    }

    /**
     * Asserts the statement is rejected with the given message fragment, and - when
     * {@code expectedPosition} is not -1 - at that exact character offset. Accepts CairoException
     * as well as SqlException because the partition helpers raise a recoverable CairoException for
     * a WHERE clause that matches nothing.
     */
    private void assertRejected(String sql, int expectedPosition, String expectedMessageFragment) {
        try {
            execute(sql);
            Assert.fail("expected SqlException for " + sql);
        } catch (SqlException | CairoException e) {
            Assert.assertTrue(
                    "[sql=" + sql + "] expected message containing '" + expectedMessageFragment
                            + "', got: " + e.getMessage(),
                    e.getMessage().contains(expectedMessageFragment)
            );
            if (expectedPosition != -1) {
                final int actualPosition = e instanceof SqlException
                        ? ((SqlException) e).getPosition()
                        : ((CairoException) e).getPosition();
                Assert.assertEquals("[sql=" + sql + "] error position", expectedPosition, actualPosition);
            }
        } catch (Exception e) {
            throw new AssertionError("[sql=" + sql + "] unexpected exception", e);
        }
    }

    /**
     * Asserts the timeline's logical entries are exactly the {@code (maxTimestamp, effective row
     * position)} pairs given, in order. The position is the effective one, so it carries the
     * retention corrections the difference array holds rather than what the entry itself stores.
     */
    private void assertLadder(LiveViewInstance instance, long... expectedPairs) {
        final LongList expected = new LongList();
        for (long value : expectedPairs) {
            expected.add(value);
        }
        final LongList actual = snapshotCheckpointLadder(instance);
        Assert.assertEquals("checkpoint ladder (maxTimestamp, effective position)", expected.toString(), actual.toString());
    }

    private void assertRetentionMarker(TableToken lvToken, boolean expected) {
        try (Path path = new Path()) {
            Assert.assertEquals(
                    "retention marker presence at " + checkpointsDir(path, lvToken),
                    expected,
                    LiveViewRetentionMarker.exists(configuration.getFilesFacade(), path)
            );
        }
    }

    /**
     * Opens a fresh inner cursor over {@code SELECT * FROM lv} and asserts the routing mode it
     * picked, which is how a test tells a rebuilt in-memory tier (seam) from an un-stamped one
     * (disk-only).
     */
    private void assertRoutingMode(int expectedRoutingMode) throws SqlException {
        try (
                RecordCursorFactory factory = select("SELECT * FROM lv");
                LiveViewRecordCursor cursor = (LiveViewRecordCursor) unwrapLvFactory(factory).getCursor(sqlExecutionContext)
        ) {
            Assert.assertEquals("live view cursor routing mode", expectedRoutingMode, cursor.routingMode());
        }
    }

    private void assertShowCreateContains(String viewName, String expectedFragment) throws SqlException {
        final String ddl = showCreateLiveView(viewName);
        Assert.assertTrue(
                "SHOW CREATE LIVE VIEW " + viewName + " must contain '" + expectedFragment + "', got: " + ddl,
                ddl.contains(expectedFragment)
        );
    }

    private void assertShowCreateContains(String expectedFragment) throws SqlException {
        assertShowCreateContains("lv", expectedFragment);
    }

    /**
     * Drops the view, re-executes its own SHOW CREATE output and asserts the second SHOW CREATE
     * matches the first. A literal string compare would pass for output that cannot be re-executed;
     * this proves the TTL clause the factory emits parses back through CREATE LIVE VIEW.
     */
    private void assertTimelineExists(TableToken lvToken, boolean expected) {
        try (Path path = new Path()) {
            checkpointsDir(path, lvToken).concat(LiveViewCheckpointLayout.TIMELINE_FILE_NAME).$();
            Assert.assertEquals("timeline presence at " + path, expected, configuration.getFilesFacade().exists(path.$()));
        }
    }

    private void assertShowCreateRoundTrips(String viewName) throws SqlException {
        final String originalDdl = showCreateLiveView(viewName);
        execute("DROP LIVE VIEW " + viewName);
        execute(originalDdl);
        TestUtils.assertEquals(originalDdl, showCreateLiveView(viewName));
    }

    /**
     * Drives the refresh job until the live view's own table holds at least {@code expectedRows}.
     * <p>
     * {@code driveRefreshToQuiescence} cannot serve here: it stops at the first pass that finds no
     * work, which is routinely the pass before the {@code FLUSH EVERY} deadline comes round, so the
     * newest rows are still in the un-flushed lead. They are visible to a query either way, but a
     * partition of the durable tier only exists once the flush has written it.
     */
    /**
     * Drives the refresh job, one clock advance per pass, until {@code condition} holds. Same
     * shape as {@link #driveUntilDurableRowCount}, for the conditions a row count cannot express.
     */
    private void driveUntil(LiveViewRefreshJob job, BooleanSupplier condition, String failure) {
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            drainJob(job);
            drainWalQueue();
        }
        Assert.fail(failure + " within " + REFRESH_QUIESCENCE_PASSES + " passes");
    }

    private void driveUntilDurableRowCount(LiveViewRefreshJob job, long expectedRows) {
        final TableToken lvToken = engine.verifyTableName("lv");
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            try (TableReader reader = engine.getReader(lvToken)) {
                if (reader.size() >= expectedRows) {
                    return;
                }
            }
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            drainJob(job);
            drainWalQueue();
        }
        Assert.fail("the live view's table never reached " + expectedRows + " durable rows");
    }

    /**
     * Sequences a {@code DROP PARTITION} into the live view's WAL without compiling it.
     * <p>
     * This is the only way to reach {@link io.questdb.cairo.TableWriter#removePartition(long)}'s own
     * guard with an active target: {@code compileAlterLiveView} refuses one up front, and the WAL
     * replay recompiles the same text with that reader-based check switched off. The replay reads
     * the SQL text alone, so the operation built here only has to carry the view's table id.
     */
    private void sequenceRawDropPartition(TableToken lvToken, String partitionName) {
        final int tableId;
        try (TableMetadata metadata = engine.getTableMetadata(lvToken)) {
            tableId = metadata.getTableId();
        }
        final AlterOperationBuilder builder = new AlterOperationBuilder().ofDropPartition(0, lvToken, tableId);
        builder.addPartitionToList(ts(partitionName), 0);
        final AlterOperation op = builder.build();
        op.withContext(sqlExecutionContext);
        op.withSqlStatement("ALTER LIVE VIEW " + lvToken.getTableName() + " DROP PARTITION LIST '" + partitionName + "'");
        try (WalWriter walWriter = engine.getWalWriter(lvToken)) {
            walWriter.apply(op, true);
        }
    }

    private String showCreateLiveView(String viewName) throws SqlException {
        printSql("SHOW CREATE LIVE VIEW " + viewName + ";");
        return sink.toString().replace("ddl\n", "");
    }

    /**
     * Drives the refresh job until the live view's own WAL is fully applied.
     * <p>
     * {@code driveRefreshToQuiescence} cannot serve here: it stops on the first pass that reports no
     * work, and sequencing an ALTER resets the view's {@link SeqTxnTracker} to UNINITIALIZED, which
     * makes the refresh job's lagging scan skip the view for exactly one pass. Keying the loop on
     * the applied txn instead makes it wait for the outcome the test is about.
     */
    private void driveLiveViewWalApply(LiveViewRefreshJob job) {
        final TableToken lvToken = engine.verifyTableName("lv");
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);
            if (tracker.isInitialised() && tracker.getWriterTxn() >= tracker.getSeqTxn()) {
                return;
            }
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            drainJob(job);
            drainWalQueue();
        }
        Assert.fail("live view WAL was not fully applied within " + REFRESH_QUIESCENCE_PASSES + " passes");
    }

    /**
     * Inserts one base row at midnight of {@code day} and drives the job until the row is in the
     * live view's own table and the refresh is idle, so the flush - and, at a one-row cadence, its
     * seal - has run. {@code expectedDurableRows} is the table size the flush must reach.
     */
    private void flushRow(LiveViewRefreshJob job, String day, int x, long expectedDurableRows) throws Exception {
        execute("INSERT INTO base VALUES ('" + day + "T00:00:00.000000Z', " + x + ")");
        driveUntilDurableRowCount(job, expectedDurableRows);
        driveRefreshToQuiescence(job);
    }

    /**
     * Rebuilds the registry from disk, as a restart does, and drives the first refresh cycle,
     * asserting it took the applied-base rebuild for {@code cause} and cleared the retention marker.
     */
    private void restartAndAssertRebuiltFromAppliedBase(String cause) throws Exception {
        final LogCapture capture = new LogCapture();
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        capture.start();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
            capture.waitFor("live view restart rebuilding from applied base [view=lv, cause=" + cause);
        } finally {
            capture.stop();
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertTrue(instance.isCheckpointRestoreSucceeded());
        Assert.assertFalse(instance.hasPendingPartitionRemovals());
        assertRetentionMarker(engine.verifyTableName("lv"), false);
        assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
    }

    /**
     * Rebuilds the registry from disk, as a restart does, and drives the first refresh cycle,
     * asserting it restored the runtime from the checkpoint timeline rather than rebuilding it from
     * the applied base - which is what tells a reconciled timeline from a retired one.
     */
    private void restartAndAssertRestoredFromTimeline() throws Exception {
        final LogCapture capture = new LogCapture();
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        capture.start();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
            capture.waitFor("restored live view from checkpoint timeline [view=lv");
            capture.assertNotLogged("live view restart rebuilding from applied base [view=lv");
        } finally {
            capture.stop();
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertTrue(instance.isCheckpointRestoreSucceeded());
        Assert.assertNotEquals("a restore stamps the restore time", Numbers.LONG_NULL, instance.getHeadCheckpointRestoreMicros());
        Assert.assertFalse(instance.hasPendingPartitionRemovals());
        assertRetentionMarker(engine.verifyTableName("lv"), false);
        assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
    }

    private void assertParquetPartitionCount(int expected) throws Exception {
        assertQuery("SELECT count() FROM table_partitions('lv') WHERE isParquet")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + expected + "\n");
    }

    /**
     * Asserts the three-day fixture of the conversion round-trip through both read paths: the
     * record cursor, which streams every column, and the page-frame cursor a scalar aggregate
     * drives.
     */
    private void assertParquetViewRows() throws Exception {
        assertQuery("SELECT * FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tx\tsym\tv\trn
                        1970-01-01T00:00:00.000000Z\t1\ta\talpha\t1
                        1970-01-02T00:00:00.000000Z\t2\tb\tbeta\t1
                        1970-01-03T00:00:00.000000Z\t3\ta\tgamma\t2
                        """);
        assertQuery("SELECT sum(x), count() FROM lv")
                .noLeakCheck().noRandomAccess().expectSize()
                .returns("sum\tcount\n6\t3\n");
    }

    /**
     * A base and a view carrying a SYMBOL and a VARCHAR beside the fixed-width columns, so a
     * staging pass over the view's own table exercises the raw symbol id, the var-size payload
     * append and the fixed-width copy in one row.
     */
    private void createParquetBaseAndView(String inMemory) throws Exception {
        createParquetBaseAndView(inMemory, "PARTITION BY DAY");
    }

    private void createParquetBaseAndView(String inMemory, String partitionByClause) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL, v VARCHAR) TIMESTAMP(ts) " + partitionByClause + " WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY " + inMemory + " " + partitionByClause + " START FROM NOW AS " +
                "(SELECT ts, x, sym, v, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }

    private void createBaseAndView(String partitionByClause) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s " + partitionByClause + " START FROM NOW AS " +
                "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }
}
