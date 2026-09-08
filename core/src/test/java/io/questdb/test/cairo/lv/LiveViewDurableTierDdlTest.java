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
import io.questdb.cairo.lv.LiveViewInstance;
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
 * counter, the conservative checkpoint-timeline disposition and the durable retention marker.
 * Precise per-root timeline retention, Parquet-aware tier rebuild, seed recovery and replica
 * propagation belong to the later stages.
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
    public void testDropPartitionOnIdleViewRebuildsTierAndRetiresTimeline() throws Exception {
        // A DROP on a view with nothing to flush lands through the lagging scan's apply retry:
        // rows leave the durable tier without a base commit, the in-memory tier is rebuilt
        // from the surviving table so seam routing resumes, the lifetime counter drops by
        // exactly the removed rows, and the checkpoint timeline - whose roots still count
        // those rows - is retired rather than left to mislead a restart.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
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
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(3, instance.getLvRowsTotal());

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
                assertTimelineExists(lvToken, false);
                assertRetentionMarker(lvToken, false);
                // The tier was rebuilt from the surviving table: a fresh cursor seams again
                // instead of falling back to disk-only.
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);

                // The next flush opens a fresh history at the corrected position, and the
                // removal never registers as an unexplained row-count mismatch.
                execute("INSERT INTO base VALUES ('1970-01-04T00:00:00.000000Z', 4)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(3, instance.getLvRowsTotal());
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
                // The retire and the fresh seal happen in the same flush, so the timeline is
                // back, at the corrected position, and nothing counted as a mismatch.
                assertTimelineExists(lvToken, true);
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
        });
    }

    private static Path checkpointsDir(Path path, TableToken lvToken) {
        return path.of(configuration.getDbRoot())
                .concat(lvToken)
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
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

    private void createBaseAndView(String partitionByClause) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s " + partitionByClause + " START FROM NOW AS " +
                "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }
}
