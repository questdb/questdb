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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionRemovalEvents;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRetentionMarker;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalUtils;
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
 * {@code IN MEMORY} window, the row positions an out-of-order repair reads back through a
 * converted partition, the rows such a repair brings back over a period a removal already took,
 * a {@code DROP PARTITION} queued between such a repair's capture and the apply of what it
 * publishes - a replacement, or the upsert a keyed repair publishes instead - and what a removal
 * taken while the view is still SEEDING does to the sweep's resume, including a failure at each
 * step of the reset that resume falls back to. Replica propagation belongs to a later stage.
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
    public void testCreateLiveViewTtlLandsInBothDefinitionCopies() throws Exception {
        // The TTL reaches _meta and both _lv copies. The sequencer-directory copy is the one that
        // travels: a replica rebuilds the view's table from it alone - _meta does not ship with it
        // and LV WAL never replicates - so a TTL held only in _meta would be lost in the crossing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s TTL 3 DAYS PARTITION BY DAY START FROM NOW AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            // A view with no TTL clause persists the same 0 the table's _meta carries, so the two
            // stay comparable without a separate "unset" state.
            execute("CREATE LIVE VIEW lv_no_ttl FLUSH EVERY 1s PARTITION BY DAY START FROM NOW AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");

            assertTtl("lv", 72);
            assertDefinitionTtl("lv", 72);
            assertTtl("lv_no_ttl", 0);
            assertDefinitionTtl("lv_no_ttl", 0);
        });
    }

    @Test
    public void testSetTtlRewritesBothDefinitionCopies() throws Exception {
        // _lv is no longer write-once: SET TTL rewrites it, or the definition would describe the
        // TTL the view had at CREATE for the rest of its life - and it is the definition, not
        // _meta, that a replica genesises its own copy of the view from.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDefinitionTtl("lv", 0);

                execute("ALTER LIVE VIEW lv SET TTL 4 WEEKS");
                driveLiveViewWalApply(job);
                assertTtl(28 * 24);
                assertDefinitionTtl("lv", 28 * 24);
                // The registered instance's own definition is the object the rewrite went through,
                // so the running refresh worker sees the new value without re-reading _lv.
                Assert.assertEquals(
                        28 * 24,
                        engine.getLiveViewRegistry().getViewInstance("lv").getDefinition().getTtlHoursOrMonths()
                );

                // Months take the negative encoding through the definition too.
                execute("ALTER LIVE VIEW lv SET TTL 1 YEAR");
                driveLiveViewWalApply(job);
                assertTtl(-12);
                assertDefinitionTtl("lv", -12);

                // Clearing is a rewrite like any other: 0 has to land, not be skipped as "unset".
                execute("ALTER LIVE VIEW lv SET TTL 0 HOURS");
                driveLiveViewWalApply(job);
                assertTtl(0);
                assertDefinitionTtl("lv", 0);

                execute("ALTER LIVE VIEW lv SET TTL 2d");
                driveLiveViewWalApply(job);
                assertDefinitionTtl("lv", 48);
            }

            // A restart loads the definition off disk, which is what proves the rewrite was
            // durable rather than an in-memory patch.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            Assert.assertEquals(
                    48,
                    engine.getLiveViewRegistry().getViewInstance("lv").getDefinition().getTtlHoursOrMonths()
            );
        });
    }

    @Test
    public void testSetTtlRewritesDefinitionWithNoRegisteredInstance() throws Exception {
        // The shape a node with refresh disabled applies every ALTER in: no instance in the
        // registry, the global ApplyWal2TableJob driving the apply. The writer has no in-memory
        // definition to lend it, so it reads _lv back off disk; without that fallback the rewrite
        // would silently skip exactly the nodes that cannot repair it later.
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertDefinitionTtl("lv", 0);

            engine.getLiveViewRegistry().clear();
            Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
            execute("ALTER LIVE VIEW lv SET TTL 4 WEEKS");
            try (ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 1)) {
                applyJob.applyWalDirect(lvToken, Job.RUNNING_STATUS);
            }
            assertTtl(28 * 24);
            assertDefinitionTtl("lv", 28 * 24);

            engine.buildViewGraphs();
            Assert.assertEquals(
                    28 * 24,
                    engine.getLiveViewRegistry().getViewInstance("lv").getDefinition().getTtlHoursOrMonths()
            );
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
    public void testDropPartitionRejectsAnActiveSplitPartition() throws Exception {
        // The guard reads the frontier as the logical floor of the table's max timestamp, and a
        // live view whose newest ATTACHED partition is a physical split is the only shape that
        // tells that apart from the newest attached partition's own timestamp. The split here
        // carries 03:03:50.000001, a timestamp no partition name spells: a guard that compared
        // against it would pass every other case in this class and let 'DROP PARTITION LIST
        // 2026-01-01T03' through - taking the frontier the refresh pipeline writes into, since
        // the removal drops every physical part of the logical partition it names.
        //
        // A localized out-of-order repair is what produces the split. Its replacement carries a
        // finite high bound - the boundary the recomputation converged at - so hour 03 keeps a
        // data prefix below the correction AND a data suffix above it, which is the shape the
        // writer splits along rather than rewriting the partition whole. A replacement that runs
        // to positive infinity - an anchored resume, or a head-miss replay that found no
        // convergence boundary - leaves no data suffix in the tail partition and never splits it.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Hour 02 is the partition that stays droppable throughout. Hour 03 is the one
                // that splits, and it needs a prefix long enough to be worth splitting: the
                // writer splits only when the prefix outweighs twice the rows the merge and the
                // suffix carry. One commit per row, so the ladder holds a root per row and the
                // repair below can localize to a boundary just under the correction.
                flushOneRow(job, "2026-01-01T02:00:00.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                for (int i = 0; i < 30; i++) {
                    final String timestamp = String.format("2026-01-01T03:%02d:%02d.000000Z", i / 6, (i % 6) * 10);
                    flushOneRow(job, timestamp, i + 3, i + 3);
                }

                // The correction sits 55 seconds under the frontier, so the 30-second frame
                // converges at 03:04:25 and the replay leaves 03:04:30 onwards alone.
                setCurrentMicros(ts("2026-01-01T03:04:50.000000Z"));
                execute("INSERT INTO base VALUES ('2026-01-01T03:03:55.000000Z', 'a', 100)");
                driveRefreshToQuiescence(job);

                // The shape everything below stands on: hour 03 is two physical partitions and
                // the newest attached one is the split, whose name carries a time of day the
                // hourly partition name never does.
                assertQuery("SELECT name, numRows FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name\tnumRows
                                2026-01-01T02\t2
                                2026-01-01T03\t24
                                2026-01-01T030350-000001\t7
                                """);
                assertSqlCursors(
                        "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                                "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base",
                        "SELECT ts, sym, s FROM lv"
                );

                final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn();
                final String expected = "cannot drop the active partition of a live view [partition=2026-01-01T03:00:00.000000Z]";

                // The hour itself, which is what a user reads off table_partitions() for the
                // lower half of the split pair. Dropping it would take both halves.
                assertRejected(
                        "ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T03'",
                        "ALTER LIVE VIEW lv DROP PARTITION LIST ".length(),
                        expected
                );
                // The split's own directory name. A non-FORCE DROP parses the hour out of it and
                // ignores the split suffix, so it names the same logical partition.
                assertRejected("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T030350-000001'", expected);
                // FORCE is the one selector that would address the split half on its own, and a
                // live view rejects it outright - so there is no spelling that reaches it.
                assertRejected(
                        "ALTER LIVE VIEW lv FORCE DROP PARTITION LIST '2026-01-01T030350-000001'",
                        "FORCE DROP PARTITION is not supported on live views"
                );
                // WHERE selects logical partitions, so one that reaches the hour is rejected...
                assertRejected("ALTER LIVE VIEW lv DROP PARTITION WHERE ts >= '2026-01-01T03'", expected);
                // ...and one that falls entirely inside the split matches nothing at all.
                assertRejected(
                        "ALTER LIVE VIEW lv DROP PARTITION WHERE ts >= '2026-01-01T03:04:00.000000Z'",
                        "no partitions matched WHERE clause"
                );
                Assert.assertEquals(
                        "a rejected DROP PARTITION must not reach the live view's WAL",
                        seqTxn,
                        engine.getTableSequencerAPI().getTxnTracker(lvToken).getSeqTxn()
                );

                // The authoritative check, reached the way a replicated command or an older
                // binary's statement reaches it: sequenced straight into the view's WAL, past the
                // compiler. The replay recompiles the statement's text with the compile-time
                // check switched off, and a non-FORCE parse floors the split suffix away, so both
                // spellings of the frontier - the hour, and the split's own physical timestamp -
                // reach the writer as hour 03. The writer refuses each against a partition set
                // whose newest member is the split, and the failure is tolerated rather than
                // suspending the view.
                assertRawDropPartitionTolerated(job, lvToken, "2026-01-01T03");
                assertRawDropPartitionTolerated(job, lvToken, "2026-01-01T03:03:50.000001Z");

                Assert.assertFalse(
                        "an active-partition DROP must be tolerated, not suspend the live view",
                        engine.getTableSequencerAPI().isSuspended(lvToken)
                );
                assertQuery("SELECT name, numRows FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name\tnumRows
                                2026-01-01T02\t2
                                2026-01-01T03\t24
                                2026-01-01T030350-000001\t7
                                """);

                // Everything below the frontier's own logical partition stays droppable while the
                // split stands, and the view keeps applying: a tolerated failure is not a stall.
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T02'");
                driveLiveViewWalApply(job);
                assertQuery("SELECT name, numRows FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name\tnumRows
                                2026-01-01T03\t24
                                2026-01-01T030350-000001\t7
                                """);
                assertSqlCursors(
                        "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                                "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s " +
                                "FROM base WHERE ts >= '2026-01-01T03:00:00.000000Z'",
                        "SELECT ts, sym, s FROM lv"
                );
                assertNoRefreshFaults("lv");
            }
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
    public void testDropPartitionAgainstUnflushedLeadLandsWithTheFlushAndRebuildsTier() throws Exception {
        // A DROP sequenced while the view holds an un-flushed lead cannot land through the lagging
        // scan's retry (hasPendingLiveViewApply defers to the flush while a lead is pending), so it
        // waits for the FLUSH EVERY tick and the flush's own inline apply drains it together with
        // the lead's block. That apply advances the writer txn by two, so the flush must not
        // re-stamp the slot - the DROP's rows sit under the slot's band and the band is no longer
        // the table's trailing rows - and, since the LV WAL is fully applied afterwards, it must
        // rebuild the tier from the surviving table in the same cycle rather than leave the view
        // disk-only until the next base commit. The lifetime counter and the timeline are
        // reconciled in the same flush, and the seal lands on top of the corrected head.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
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
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);
                assertLadder(instance, ts("1970-01-03"), 3);

                // Drain a base row into the lead without moving the clock: quiescence returned on
                // the tick after the last flush, so the FLUSH EVERY 1s deadline is not due and the
                // row stays in RAM.
                execute("INSERT INTO base VALUES ('1970-01-04T00:00:00.000000Z', 4)");
                drainWalQueue();
                drainJob(job);
                Assert.assertTrue("the row must sit in the un-flushed lead", instance.getLeadRowCount() > 0);
                assertQuery("SELECT count() FROM table_partitions('lv')").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
                final long writerTxnBefore = tracker.getWriterTxn();

                // The DROP is sequenced but nothing applies it: the retry path defers to the flush
                // while the lead is pending, and a tick at the same clock flushes nothing.
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                drainWalQueue();
                drainJob(job);
                Assert.assertTrue("the DROP must wait for the flush", tracker.getSeqTxn() > tracker.getWriterTxn());
                Assert.assertEquals(writerTxnBefore, tracker.getWriterTxn());
                assertQuery("SELECT count() FROM table_partitions('lv')").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

                // The flush's apply lands the DROP and the lead's block in one pass.
                setCurrentMicros(currentMicros + 1_000_000);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals("the flush must land the DROP and its own block", writerTxnBefore + 2, tracker.getWriterTxn());
                Assert.assertEquals("the LV WAL must be fully applied", tracker.getSeqTxn(), tracker.getWriterTxn());
                Assert.assertEquals(0, instance.getLeadRowCount());
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                1970-01-04
                                """);
                // The tier was rebuilt from the surviving table in the same flush: no base commit
                // has arrived since, and a fresh cursor seams again instead of reading disk-only.
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                Assert.assertFalse("the flush must rebuild the tier itself", instance.isTierStale());
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
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
                assertQuery("SELECT min(x), max(x) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("min\tmax\n2\t4\n");
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                Assert.assertEquals("the dropped row must leave the lifetime counter", 3, instance.getLvRowsTotal());
                assertRetentionMarker(lvToken, false);
                // The root above the dropped day survives, lowered by the row that went, and the
                // seal appends the flush's own frontier on top of it; nothing counted as a mismatch.
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(0, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-03"), 2, ts("1970-01-04"), 3);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                driveRefreshToQuiescence(job);
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                assertNoRefreshFaults("lv");
            }

            // A restart restores from the reconciled timeline, and the dropped day stays dropped.
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
    public void testTwoDropsDrainedTogetherCountTheirRemovalsOnce() throws Exception {
        // Two DROP PARTITION statements sequenced back to back and drained in one apply. The
        // writer publishes its committed events per transaction and the apply job accumulates
        // them across the whole drain, so the drain hands the refresh worker two events at once -
        // and each has to lower the lifetime counter and the ladder exactly once. Subtracting a
        // batch twice would empty the view here, and subtracting it once for the batch rather
        // than once per event would leave the counter one row high.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int day = 1; day <= 4; day++) {
                    flushRow(job, "1970-01-0" + day, day, day);
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                execute("ALTER LIVE VIEW lv DROP PARTITION WHERE ts >= '1970-01-03' AND ts < '1970-01-04'");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);
                Assert.assertEquals(
                        "both removals must still be unapplied, so one drain takes them together",
                        2,
                        tracker.getSeqTxn() - tracker.getWriterTxn()
                );
                driveLiveViewWalApply(job);

                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n4\n");
                Assert.assertEquals(2, instance.getLvRowsTotal());
                assertTimelineExists(lvToken, true);
                Assert.assertEquals("one retired root per removed partition", 2, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-02"), 1, ts("1970-01-04"), 2);
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n4\n");
            Assert.assertEquals(2, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRemovalCommittedBeforeAFailureInTheSameTransactionCountsOnce() throws Exception {
        // One DROP statement over two partitions, with the second partition's retention marker
        // refusing to publish. removePartition() commits once per logical partition, so the
        // transaction takes 1970-01-01 durably and only then fails on 1970-01-02: a transaction
        // that removed rows and afterwards threw. Nothing un-applies the commit that went
        // through, so the event it published has to reach the refresh worker even though the
        // apply ends in a suspension - which is why the apply job accumulates the writer's
        // committed log in a finally rather than after a clean return. Dropping the event there
        // would leave the lifetime counter one row above a table that already shrank, with no
        // pending evidence to explain it, and the next seal would report the drift and retire
        // the history instead of restoring from it.
        final AtomicInteger markerPublishes = new AtomicInteger();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                // The second partition of the statement, and only it: the replay after RESUME
                // WAL publishes no marker at all, and a later removal must be able to.
                if (Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.RETENTION_MARKER_FILE_NAME)
                        && markerPublishes.incrementAndGet() == 2) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        }, () -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int day = 1; day <= 5; day++) {
                    flushRow(job, "1970-01-0" + day, day, day);
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01', '1970-01-02'");
                driveUntil(job, () -> engine.getTableSequencerAPI().isSuspended(lvToken), "the live view was not suspended");

                // Half the statement is durable, and the half that is not leaves the transaction
                // unapplied - the writer rolled its seqTxn back so the replay redelivers it.
                Assert.assertEquals(
                        "the failed transaction must stay unapplied",
                        1,
                        tracker.getSeqTxn() - tracker.getWriterTxn()
                );
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                1970-01-04
                                1970-01-05
                                """);
                // The removal that did commit was reported, once.
                Assert.assertEquals(1, instance.getPendingPartitionRemovals().size());
                Assert.assertEquals(1, instance.getPendingPartitionRemovals().getTotalRemovedRows());
                // Reconciling it is deferred while the transaction is outstanding: the counter
                // may legitimately lead the table there, so nothing may be subtracted yet, and
                // the marker the removal wrote is still the live evidence of that gap.
                Assert.assertEquals(5, instance.getLvRowsTotal());
                assertRetentionMarker(lvToken, true);
                assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck().noRandomAccess().returns("view_status\nsuspended\n");

                // Driving on while the reconcile is deferred redelivers nothing: the apply
                // clears the writer's log as it takes each event, so a second apply cannot hand
                // the same removal over again and inflate what the reconcile will subtract.
                for (int i = 0; i < 4; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }
                Assert.assertEquals(1, instance.getPendingPartitionRemovals().size());
                Assert.assertEquals(5, instance.getLvRowsTotal());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);

                // The replay re-runs the whole statement, and its first partition is already
                // gone: removePartition() answers false for it, which is a WAL-tolerable failure,
                // so the transaction is marked applied and the rest of the LIST is abandoned.
                // 1970-01-02 therefore stays attached - the statement is not resumed where it
                // stopped - and this is what a partly-applied multi-partition DROP leaves behind.
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));
                assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck().noRandomAccess().returns("view_status\nactive\n");
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n3\n4\n5\n");

                // Exactly the one row that actually went is off the counter and off the ladder.
                Assert.assertEquals(4, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertTimelineExists(lvToken, true);
                Assert.assertEquals("one retired root, for the partition that went", 1, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-02"), 1, ts("1970-01-03"), 2, ts("1970-01-04"), 3, ts("1970-01-05"), 4);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n3\n4\n5\n");
            Assert.assertEquals(4, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRemovalHeldAcrossAPartialApplyCountsOnceAfterTheResume() throws Exception {
        // Two DROP statements sequenced back to back, with the second one's retention marker
        // refusing to publish. One drain takes both, applies the first and stops on the second:
        // a partial apply, which leaves the worker holding an event for a removal that is
        // already durable while a transaction the view committed is still outstanding. That is
        // the state the reconcile defers in, so the event has to survive the wait and every
        // apply that happens during it. What the resume then has to produce is each removal
        // subtracted exactly once, across the two applies that delivered them - a batch counted
        // per delivery would take the first partition's row twice and leave the counter short.
        final AtomicInteger markerPublishes = new AtomicInteger();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                // The second statement's first attempt, and only it: its replay after RESUME WAL
                // is the third publish and must go through.
                if (Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.RETENTION_MARKER_FILE_NAME)
                        && markerPublishes.incrementAndGet() == 2) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        }, () -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int day = 1; day <= 5; day++) {
                    flushRow(job, "1970-01-0" + day, day, day);
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-02'");
                Assert.assertEquals(
                        "both removals must still be unapplied, so one drain takes them together",
                        2,
                        tracker.getSeqTxn() - tracker.getWriterTxn()
                );
                driveUntil(job, () -> engine.getTableSequencerAPI().isSuspended(lvToken), "the live view was not suspended");

                Assert.assertEquals(
                        "the drain must have applied the first removal and stopped on the second",
                        1,
                        tracker.getSeqTxn() - tracker.getWriterTxn()
                );
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                1970-01-04
                                1970-01-05
                                """);
                Assert.assertEquals(1, instance.getPendingPartitionRemovals().size());
                Assert.assertEquals(1, instance.getPendingPartitionRemovals().getTotalRemovedRows());
                Assert.assertEquals(5, instance.getLvRowsTotal());
                assertRetentionMarker(lvToken, true);

                // The deferral holds across further applies, and nothing is delivered twice.
                for (int i = 0; i < 4; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }
                Assert.assertEquals(1, instance.getPendingPartitionRemovals().size());
                Assert.assertEquals(5, instance.getLvRowsTotal());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);

                // The rest of the drain lands, and the two events - one held over the wait, one
                // from this apply - reconcile together.
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n3\n4\n5\n");
                Assert.assertEquals(3, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertTimelineExists(lvToken, true);
                Assert.assertEquals("one retired root per removed partition", 2, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-03"), 1, ts("1970-01-04"), 2, ts("1970-01-05"), 3);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n3\n4\n5\n");
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRemovalCommittedBeforeAToleratedRejectionCountsOnce() throws Exception {
        // The other end of the same drain: a removal that commits, and then a second transaction
        // the writer refuses recoverably. The active-partition guard is that refusal - a WAL
        // command failure the apply tolerates, which marks the transaction applied and leaves the
        // view running rather than suspending it. The drain therefore ends clean, at a fully
        // applied boundary, and the reconcile runs on the spot: the removal that did happen has
        // to be subtracted exactly once, and the one the guard refused not at all. A drain that
        // discarded its committed events on the way past a failed transaction would leave the
        // counter one row high; one that counted the refusal as a removal would leave it short.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int day = 1; day <= 4; day++) {
                    flushRow(job, "1970-01-0" + day, day, day);
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);

                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                    // 1970-01-04 is the durable frontier, so only a statement that never met the
                    // compiler can name it. This is the shape a replicated command takes.
                    sequenceRawDropPartition(lvToken, "1970-01-04");
                    Assert.assertEquals(
                            "both transactions must still be unapplied, so one drain takes them together",
                            2,
                            tracker.getSeqTxn() - tracker.getWriterTxn()
                    );
                    driveLiveViewWalApply(job);
                    capture.drain();
                    capture.assertLoggedRE("tolerated WAL command failure \\[table=" + lvToken.getDirName()
                            + ", seqTxn=\\d+, command=ALTER TABLE, "
                            + "error=cannot drop the active partition of a live view "
                            + "\\[partition=1970-01-04T00:00:00\\.000000Z]");
                } finally {
                    capture.stop();
                }
                driveRefreshToQuiescence(job);

                Assert.assertFalse(
                        "a recoverable rejection must not suspend the live view",
                        engine.getTableSequencerAPI().isSuspended(lvToken)
                );
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n3\n4\n");
                Assert.assertEquals(3, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertTimelineExists(lvToken, true);
                Assert.assertEquals("only the removal that happened retires a root", 1, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-02"), 1, ts("1970-01-03"), 2, ts("1970-01-04"), 3);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n3\n4\n");
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRepairResurrectsOnlyTheDroppedPartitionItsIntervalCovers() throws Exception {
        // The documented DROP PARTITION semantics: the removal takes durable rows now, and only
        // now. A later out-of-order base commit whose repair interval overlaps a dropped period
        // re-derives the rows inside it off the base, because the replay computes the stream
        // again rather than reading what the view's table still holds. What must not travel with
        // them is the rest of the removal: a dropped partition below the repair's anchor stays
        // dropped, since the replay starts at the anchor and the replacement range never reaches
        // down to it.
        //
        // Two separated hours go and the out-of-order row lands in the upper one, so the plan's
        // anchor is the root in the surviving hour between them: hour 03 comes back, hour 01 does
        // not. The resurrected row also comes back renumbered - the state that anchor carries
        // counted the hour-01 row the drop removed, and the out-of-order row now sits ahead of
        // hour 03 in the stream - which is the difference between re-deriving a row and restoring
        // one, and the reason a user who wants retention that survives recovery needs TTL.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // One commit - and so one logical root - per row, one row per hour.
                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 3, 3);
                flushOneRow(job, "2026-01-01T04:00:10.000000Z", 4, 4);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T01', '2026-01-01T03'");
                driveLiveViewWalApply(job);

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                2026-01-01T02:00:10.000000Z\t2\t2
                                2026-01-01T04:00:10.000000Z\t4\t4
                                """);
                assertLadder(instance, ts("2026-01-01T02:00:10.000000Z"), 1, ts("2026-01-01T04:00:10.000000Z"), 2);
                Assert.assertEquals(2, instance.getLvRowsTotal());
                assertRetentionMarker(lvToken, false);

                // The out-of-order row lands inside dropped hour 03, above the surviving root at
                // 02:00:10 that the plan anchors on.
                execute("INSERT INTO base VALUES ('2026-01-01T03:00:05.000000Z', 'a', 100)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 4;
                            }
                        },
                        "the repair never re-emitted the dropped hour"
                );
                driveRefreshToQuiescence(job);

                // Hour 03 is back, with the out-of-order row ahead of it, and both are numbered
                // off the anchor's state - which had seen the hour-01 row too. Hour 01 itself is
                // still gone: nothing in the repair's interval covers it.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                2026-01-01T02:00:10.000000Z\t2\t2
                                2026-01-01T03:00:05.000000Z\t100\t3
                                2026-01-01T03:00:10.000000Z\t3\t4
                                2026-01-01T04:00:10.000000Z\t4\t5
                                """);
                Assert.assertEquals(4, instance.getLvRowsTotal());
                assertQuery("SELECT count() FROM lv WHERE ts < '2026-01-01T02:00:00.000000Z'")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            // And the ladder the repair published is what a restart reads back, with hour 01 still
            // missing and hour 03 still resurrected.
            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\trn
                            2026-01-01T02:00:10.000000Z\t2
                            2026-01-01T03:00:05.000000Z\t3
                            2026-01-01T03:00:10.000000Z\t4
                            2026-01-01T04:00:10.000000Z\t5
                            """);
            Assert.assertEquals(4, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRepairWithNoAnchorLeftResurrectsEveryDroppedRowBelowIt() throws Exception {
        // The same semantics in their starkest form. Dropping the oldest partitions retires every
        // root inside them, so an out-of-order row below the lowest surviving root leaves the plan
        // no anchor at all and the replay runs from the view's own lower bound. The interval is
        // then the whole view, and every dropped row inside it comes back off the base.
        //
        // The replay measures itself against a table the removal already shrank, so this is also
        // the case where the reconciled counter has to be the one the proof uses: an unreconciled
        // count would put the replacement's row-count proof two rows out and suspend the view.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, x, count(*) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 3, 3);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                execute("ALTER LIVE VIEW lv DROP PARTITION WHERE ts < '2026-01-01T03:00:00.000000Z'");
                driveLiveViewWalApply(job);

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                2026-01-01T03:00:10.000000Z\t3\t3
                                """);
                assertLadder(instance, ts("2026-01-01T03:00:10.000000Z"), 1);
                Assert.assertEquals(1, instance.getLvRowsTotal());

                // Below every surviving root, so there is nothing to anchor on and the replay
                // starts at the view's lower bound.
                execute("INSERT INTO base VALUES ('2026-01-01T01:00:05.000000Z', 'a', 100)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 4;
                            }
                        },
                        "the replay never re-derived the dropped hours"
                );
                driveRefreshToQuiescence(job);

                // Both dropped hours are back, renumbered around the out-of-order row, which is
                // exactly a recompute of the base: the removal survived only until the first
                // recovery that re-derived the period it covered.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                2026-01-01T01:00:05.000000Z\t100\t1
                                2026-01-01T01:00:10.000000Z\t1\t2
                                2026-01-01T02:00:10.000000Z\t2\t3
                                2026-01-01T03:00:10.000000Z\t3\t4
                                """);
                Assert.assertEquals(4, instance.getLvRowsTotal());
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
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
                // A fault-induced suspension is visible on the view itself, not only on
                // wal_tables(): the view is registered and valid, but nothing lands until
                // RESUME WAL, and 'active' would hide that.
                assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck().noRandomAccess().returns("view_status\nsuspended\n");

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
                assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck().noRandomAccess().returns("view_status\nactive\n");
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
    public void testRetentionLowersARootByItsExactDeltaNotAFreshCount() throws Exception {
        // Section 3.4's 100/10/5/10 counterexample, in the shape the resume anchor reads it.
        // 03:00:10 seals a root at position 3 and two more rows then land on that same
        // timestamp - a group the cadence cannot open a second boundary over, so the root
        // covers one of the three rows in its own group and has to keep looking that way.
        // Dropping hour 01 under it lowers every position above by exactly the row that
        // went: 3 becomes 2 while the table still holds 4 rows at or below 03:00:10. Healing
        // the position with a fresh count of that prefix - the repair section 3.4 rejects -
        // would have written 4, the root would have read as complete, and the repair below
        // would have resumed from state two rows short.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, count(*) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 3, 3);
                // The tie. Out-of-order detection compares strictly, so a row on the
                // frontier's own timestamp is an ordinary forward append; the seal that
                // follows it has no boundary above the head to open and is skipped.
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 4, 4);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 5, 5);
                flushOneRow(job, "2026-01-01T03:01:00.000000Z", 6, 6);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(
                        instance,
                        ts("2026-01-01T01:00:10.000000Z"), 1,
                        ts("2026-01-01T02:00:10.000000Z"), 2,
                        ts("2026-01-01T03:00:10.000000Z"), 3,
                        ts("2026-01-01T03:01:00.000000Z"), 6
                );

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T01'");
                driveLiveViewWalApply(job);

                // One row went, so every surviving position drops by one and no more. The
                // under-covered root lands at 2 against the 4 rows the table holds at or
                // below its timestamp, which is what keeps it distinguishable.
                assertLadder(
                        instance,
                        ts("2026-01-01T02:00:10.000000Z"), 1,
                        ts("2026-01-01T03:00:10.000000Z"), 2,
                        ts("2026-01-01T03:01:00.000000Z"), 5
                );
                Assert.assertEquals("the dropped hour's root, and only it", 1, readRetiredCheckpointCount(lvToken));
                Assert.assertEquals(5, instance.getLvRowsTotal());
                assertRetentionMarker(lvToken, false);
                assertNoRefreshFaults("lv");

                // An out-of-order row 30 seconds above the tie. The plan searches for the
                // newest boundary strictly below it, finds the tie root, and has to refuse
                // it: the table holds more rows at or below 03:00:10 than the root claims
                // as its whole prefix. It re-anchors on 02:00:10 instead.
                capture.start();
                try {
                    execute("INSERT INTO base VALUES ('2026-01-01T03:00:40.000000Z', 'a', 100)");
                    driveUntilDurableRowCount(job, 6);
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view resume anchor no longer covers its timestamp group, re-anchoring below it "
                            + "[view=lv, anchorMaxTs=2026-01-01T03:00:10.000000Z");
                    capture.assertLoggedRE(", lvRowPosition=2]");
                } finally {
                    capture.stop();
                }

                // Every row the repair re-emitted counts the whole tie: 03:00:40 is the
                // sixth base row under 'a', not the fourth. An anchor at the tie root would
                // have restored state that had seen one row at 03:00:10 and replayed from
                // 03:00:11, numbering 03:00:40 and 03:01:00 4 and 5 - against the 4 and 5
                // the tie's own rows already hold.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\trn
                                2026-01-01T02:00:10.000000Z\ta\t2
                                2026-01-01T03:00:10.000000Z\ta\t3
                                2026-01-01T03:00:10.000000Z\ta\t4
                                2026-01-01T03:00:10.000000Z\ta\t5
                                2026-01-01T03:00:40.000000Z\ta\t6
                                2026-01-01T03:01:00.000000Z\ta\t7
                                """);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testRestartRefusesATimelineTheRemovalMadeCountCorrect() throws Exception {
        // The same counterexample where section 3.4 states it: at restart, against the row
        // count. The head root claims 3 rows and covers one of the three that share its
        // timestamp; dropping the two rows below it leaves the table holding exactly 3, all
        // of them at or below the head's own timestamp. Every count a restore could take
        // agrees with the ladder and the ladder is still wrong, so the count proves nothing
        // and the durable marker is the whole of the evidence.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY DAY START FROM NOW AS " +
                    "(SELECT ts, sym, count(*) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "2026-01-01T00:00:00.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T00:00:01.000000Z", 2, 2);
                flushOneRow(job, "2026-01-02T00:00:00.000000Z", 3, 3);
                flushOneRow(job, "2026-01-02T00:00:00.000000Z", 4, 4);
                flushOneRow(job, "2026-01-02T00:00:00.000000Z", 5, 5);
                assertLadder(
                        engine.getLiveViewRegistry().getViewInstance("lv"),
                        ts("2026-01-01T00:00:00.000000Z"), 1,
                        ts("2026-01-01T00:00:01.000000Z"), 2,
                        ts("2026-01-02T00:00:00.000000Z"), 3
                );
            }

            // The global apply job takes the removal, as on a node with refresh disabled:
            // it writes the marker and commits the removal, and reconciles nothing.
            execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01'");
            try (ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 1)) {
                applyJob.applyWalDirect(lvToken, Job.RUNNING_STATUS);
            }

            // The coincidence, spelled out: the head's stored position, the table's row
            // count and the rows at or below the head's timestamp are all 3.
            assertLadder(
                    engine.getLiveViewRegistry().getViewInstance("lv"),
                    ts("2026-01-01T00:00:00.000000Z"), 1,
                    ts("2026-01-01T00:00:01.000000Z"), 2,
                    ts("2026-01-02T00:00:00.000000Z"), 3
            );
            Assert.assertEquals(3, lvRowCount(lvToken));
            assertTimelineExists(lvToken, true);
            assertRetentionMarker(lvToken, true);

            restartAndAssertRebuiltFromAppliedBase("pending retention marker present");
            // The rebuild re-derives from the base, so the dropped day comes back - the
            // documented DROP PARTITION recovery semantics.
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\trn
                            2026-01-01T00:00:00.000000Z\ta\t1
                            2026-01-01T00:00:01.000000Z\ta\t2
                            2026-01-02T00:00:00.000000Z\ta\t3
                            2026-01-02T00:00:00.000000Z\ta\t4
                            2026-01-02T00:00:00.000000Z\ta\t5
                            """);
            Assert.assertEquals(5, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());

            // The value the restored accumulator would have got wrong. A restore that had
            // trusted the coinciding count would resume from state that had seen one row at
            // 2026-01-02 and number this one 4.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "2026-01-03T00:00:00.000000Z", 6, 6);
                assertQuery("SELECT ts, rn FROM lv WHERE ts = '2026-01-03T00:00:00.000000Z'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("""
                                ts\trn
                                2026-01-03T00:00:00.000000Z\t6
                                """);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testRetentionMarkerClearFailureLeavesTheRestartConservative() throws Exception {
        // The step after the publication. The corrected generation is durable - the ladder on
        // disk already accounts for the removal - but the unlink of the marker that guarded
        // the window between the two fails. The marker has no staleness rule, so the next
        // restart spends one applied-base rebuild rather than restoring from a timeline that
        // is in fact correct. That is the direction the failure has to take, and the counter
        // comes out of it matching the table exactly once: the rebuild re-seats it from the
        // table it just rewrote rather than subtracting the removal a second time.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final AtomicBoolean swallowMarkerRemoval = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (swallowMarkerRemoval.get()
                        && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.RETENTION_MARKER_FILE_NAME)) {
                    return true;
                }
                return super.removeQuiet(name);
            }
        }, () -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                flushRow(job, "1970-01-02", 2, 2);
                flushRow(job, "1970-01-03", 3, 3);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                swallowMarkerRemoval.set(true);
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-02'");
                driveLiveViewWalApply(job);

                // The publication itself went through: the middle root retired, the head
                // came down by the row that went, and the view carries on normally.
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("1970-01-01"), 1, ts("1970-01-03"), 2);
                Assert.assertEquals(1, readRetiredCheckpointCount(lvToken));
                Assert.assertEquals(2, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n3\n");
                assertRetentionMarker(lvToken, true);
                assertQuery("SELECT count() FROM live_views() WHERE view_status <> 'active'")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
                assertNoRefreshFaults("lv");
            }

            swallowMarkerRemoval.set(false);
            restartAndAssertRebuiltFromAppliedBase("pending retention marker present");
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n3\n");
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRetentionMarkerHasNoGenerationStalenessRule() throws Exception {
        // The repair marker is cleared by a generation strictly past the one it recorded, and
        // the retention marker deliberately carries no such rule: no later generation proves
        // a removal was accounted for, so present means live. Both records a crash can leave
        // behind are pinned here - the published one, under a generation many seals past the
        // apply it names, and the staged sibling a crash inside the publish leaves instead.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                flushRow(job, "1970-01-02", 2, 2);
                flushRow(job, "1970-01-03", 3, 3);
            }
            Assert.assertTrue(
                    "the premise: generations have been sealed over the apply the marker names",
                    readGeneration(lvToken) > 1
            );

            writeRetentionMarker(lvToken, 1);
            restartAndAssertRebuiltFromAppliedBase("pending retention marker present");
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n3\n");

            // The rebuild retired the timeline with the marker, so seal a fresh one to leave
            // the staged record something to be believed over.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-04", 4, 4);
            }
            assertTimelineExists(lvToken, true);

            writeRetentionMarker(lvToken, 2);
            stageRetentionMarker(lvToken);
            assertRetentionMarker(lvToken, true);
            restartAndAssertRebuiltFromAppliedBase("pending retention marker present");
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n3\n4\n");
        });
    }

    @Test
    public void testTtlEvictionInsideFlushReconcilesCounterAndRebuildsTier() throws Exception {
        // TTL eviction is the one removal that lands inside the flush's own commit, so the
        // writer txn advances by exactly one and the flush would otherwise re-stamp a slot
        // holding rows the table just lost. The flush must un-stamp it instead, take the
        // evicted rows off the lifetime counter, and seal the fresh history at the corrected
        // position - with no row-count mismatch recorded - and rebuild the tier from the
        // surviving table in the same cycle, so reads regain seam routing without a base commit.
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
                // The flush un-stamped the slot and rebuilt it from the surviving table in the
                // same cycle: with no further base commit, a fresh cursor seams again instead of
                // running disk-only until one arrives - which never happens on an idle view.
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                Assert.assertFalse("the flush must rebuild the tier itself", instance.isTierStale());

                // A later refresh publishes on top of the rebuilt slot.
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
    public void testTtlEvictionInsideAnO3RepairSplicesRetentionIntoOneGeneration() throws Exception {
        // The removal that lands inside an out-of-order repair's own replacement apply. The
        // repair holds a capture frozen before the replay, and the batch it meets is a TTL
        // eviction the replacement's commit made durable. Both dispositions belong to one
        // generation: the roots inside the evicted hour retire, the repaired boundaries take
        // the replay's positions, and one range-add per removed interval lowers every
        // position above it - including the repaired ones, each of which is itself a
        // surviving root above the interval.
        //
        // The branch used to refuse: the capture froze a generation counting rows the
        // eviction took, and the splice's row-count proof assumed the replacement was the
        // only thing that changed the table, so the whole ladder was retired and the view
        // rebuilt its history from the frontier. Without the change this test fails on the
        // generation, which comes back as the retired timeline's LONG_NULL.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Armed while the view is empty and the clock sits below the data, so the
                // ALTER's own apply evicts nothing: TTL judges age by the smaller of the
                // table frontier and the wall clock.
                execute("ALTER LIVE VIEW lv SET TTL 1 HOUR");
                driveLiveViewWalApply(job);

                // One commit - and so one logical root - per row. 02:00:20 and 02:00:30 are
                // the pair the repair below re-versions; 01:00:10 is the root the eviction
                // retires, 02:00:10 the reused prefix and 03:00:10 the converged suffix.
                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T02:00:20.000000Z", 3, 3);
                flushOneRow(job, "2026-01-01T02:00:30.000000Z", 4, 4);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 5, 5);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(
                        instance,
                        ts("2026-01-01T01:00:10.000000Z"), 1,
                        ts("2026-01-01T02:00:10.000000Z"), 2,
                        ts("2026-01-01T02:00:20.000000Z"), 3,
                        ts("2026-01-01T02:00:30.000000Z"), 4,
                        ts("2026-01-01T03:00:10.000000Z"), 5
                );
                final long generationBefore = readGeneration(lvToken);

                // Hour 01 ends at 02:00, so an hour past that is 03:00 - which the frontier
                // has reached. Nothing has committed since, so the eviction is owed and the
                // next commit to the view's table is the repair's own replacement.
                setCurrentMicros(ts("2026-01-01T03:00:10.000000Z"));
                execute("INSERT INTO base VALUES ('2026-01-01T02:00:15.000000Z', 'a', 100)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 5
                                        && reader.getPartitionCount() == 2
                                        && reader.getMinTimestamp() == ts("2026-01-01T02:00:10.000000Z");
                            }
                        },
                        "the repair's replacement never evicted hour 01"
                );

                // The repair's own output, with the out-of-order row folded into the frame of
                // every row within 30 seconds above it, and hour 01 gone from the table.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T02:00:10.000000Z\ta\t2.0
                                2026-01-01T02:00:15.000000Z\ta\t102.0
                                2026-01-01T02:00:20.000000Z\ta\t105.0
                                2026-01-01T02:00:30.000000Z\ta\t109.0
                                2026-01-01T03:00:10.000000Z\ta\t5.0
                                """);

                // One publication, not a retire plus a fresh seal: every root the view had
                // above the evicted hour is still addressable under its own key, and the
                // splice and the retention advanced the generation once between them.
                Assert.assertEquals(
                        "the repair and its retention must publish exactly one generation",
                        generationBefore + 1,
                        readGeneration(lvToken)
                );
                assertTimelineExists(lvToken, true);
                Assert.assertEquals("the evicted hour's root, and only it", 1, readRetiredCheckpointCount(lvToken));
                assertLadder(
                        instance,
                        ts("2026-01-01T02:00:10.000000Z"), 1,
                        ts("2026-01-01T02:00:20.000000Z"), 3,
                        ts("2026-01-01T02:00:30.000000Z"), 4,
                        ts("2026-01-01T03:00:10.000000Z"), 5
                );
                Assert.assertEquals("the evicted row must leave the lifetime counter", 5, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            // The ladder the splice published is the one a restart reads back, and the
            // evicted hour stays evicted: recovery does not re-derive it.
            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T02:00:10.000000Z\t2.0
                            2026-01-01T02:00:15.000000Z\t102.0
                            2026-01-01T02:00:20.000000Z\t105.0
                            2026-01-01T02:00:30.000000Z\t109.0
                            2026-01-01T03:00:10.000000Z\t5.0
                            """);
            Assert.assertEquals(5, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testTtlEvictionInsideAnO3ResumeSplicesRetentionIntoOneGeneration() throws Exception {
        // The same removal, on the repair's other executor. A correction two rows below the
        // frontier leaves the anchor at 03:00:50 a short tail, while localizing would warm
        // the frame up from 03:00:25 and - the frame reaching past the frontier - read to the
        // end of the base anyway, so the plan resumes from the anchor and the capture covers
        // the roots above it alone.
        //
        // That path keeps its lifetime counter rather than re-seating it from the table, so a
        // published retention has to take the evicted rows off the counter itself before the
        // post-replay seal stamps a head position on it - which is what the row count and the
        // head's own position below prove.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("ALTER LIVE VIEW lv SET TTL 1 HOUR");
                driveLiveViewWalApply(job);

                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 3, 3);
                flushOneRow(job, "2026-01-01T03:00:20.000000Z", 4, 4);
                flushOneRow(job, "2026-01-01T03:00:30.000000Z", 5, 5);
                flushOneRow(job, "2026-01-01T03:00:40.000000Z", 6, 6);
                flushOneRow(job, "2026-01-01T03:00:50.000000Z", 7, 7);
                flushOneRow(job, "2026-01-01T03:01:00.000000Z", 8, 8);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final long generationBefore = readGeneration(lvToken);

                // Hour 01 ends at 02:00, so an hour past that is 03:00 - which the frontier
                // has passed. Nothing has committed since, so the eviction is owed and the
                // next commit to the view's table is the resume's own replacement.
                setCurrentMicros(ts("2026-01-01T03:01:00.000000Z"));
                execute("INSERT INTO base VALUES ('2026-01-01T03:00:55.000000Z', 'a', 100)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 8
                                        && reader.getPartitionCount() == 2
                                        && reader.getMinTimestamp() == ts("2026-01-01T02:00:10.000000Z");
                            }
                        },
                        "the resume's replacement never evicted hour 01"
                );

                Assert.assertEquals(
                        "the resume must re-evaluate 03:00:55 and 03:01:00 and nothing else",
                        2,
                        instance.getO3ResumeReplayRows()
                );
                // The oracle: the same window over the base, minus the hour the eviction took.
                // The frame is 30 seconds wide and the hours are an hour apart, so no surviving
                // row's frame ever reached into hour 01 and filtering the input is equivalent
                // to removing the output.
                assertSqlCursors(
                        "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                                "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s " +
                                "FROM base WHERE ts >= '2026-01-01T02:00:00.000000Z'",
                        "SELECT ts, sym, s FROM lv"
                );

                Assert.assertEquals(
                        "the resume and its retention must publish exactly one generation",
                        generationBefore + 1,
                        readGeneration(lvToken)
                );
                assertTimelineExists(lvToken, true);
                Assert.assertEquals("the evicted hour's root, and only it", 1, readRetiredCheckpointCount(lvToken));
                assertLadder(
                        instance,
                        ts("2026-01-01T02:00:10.000000Z"), 1,
                        ts("2026-01-01T03:00:10.000000Z"), 2,
                        ts("2026-01-01T03:00:20.000000Z"), 3,
                        ts("2026-01-01T03:00:30.000000Z"), 4,
                        ts("2026-01-01T03:00:40.000000Z"), 5,
                        ts("2026-01-01T03:00:50.000000Z"), 6,
                        ts("2026-01-01T03:01:00.000000Z"), 8
                );
                Assert.assertEquals("the evicted row must leave the lifetime counter", 8, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            // The ladder the splice published is the one a restart reads back, and the
            // evicted hour stays evicted: recovery does not re-derive it.
            restartAndAssertRestoredFromTimeline();
            assertSqlCursors(
                    "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                            "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s " +
                            "FROM base WHERE ts >= '2026-01-01T02:00:00.000000Z'",
                    "SELECT ts, sym, s FROM lv"
            );
            Assert.assertEquals(8, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testTtlEvictionInsideAnO3TruncateReconcilesTheKeptPrefix() throws Exception {
        // The same removal on the repair route that holds no capture. The boundary bound of 1
        // declines the splice (the correction re-versions two roots), so the repair truncates
        // the timeline at R instead: it keeps the roots below 02:00:15 - 01:00:10 and 02:00:10
        // - under a repair marker, and the replacement's commit then evicts hour 01. The kept
        // prefix was published before the eviction and its positions still count the evicted
        // row, so the repair reconciles it with the ordinary retention before its post-replay
        // seal: 01:00:10 retires, 02:00:10 drops to position 1, and the fresh head at the
        // frontier is stamped off the table's size, which the eviction has already shrunk.
        //
        // Three generations - truncate, retention, seal - and the repair marker re-based on
        // the truncate's own between the first two, so a crash between the retention and the
        // seal reads as a live repair rather than a completed one. The branch used to retire
        // the whole ladder here; without the change this test fails on the generation, which
        // comes back as the retired timeline's LONG_NULL.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("ALTER LIVE VIEW lv SET TTL 1 HOUR");
                driveLiveViewWalApply(job);

                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T02:00:20.000000Z", 3, 3);
                flushOneRow(job, "2026-01-01T02:00:30.000000Z", 4, 4);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 5, 5);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(
                        instance,
                        ts("2026-01-01T01:00:10.000000Z"), 1,
                        ts("2026-01-01T02:00:10.000000Z"), 2,
                        ts("2026-01-01T02:00:20.000000Z"), 3,
                        ts("2026-01-01T02:00:30.000000Z"), 4,
                        ts("2026-01-01T03:00:10.000000Z"), 5
                );
                final long generationBefore = readGeneration(lvToken);
                final long retiredBefore = readRetiredCheckpointCount(lvToken);

                setCurrentMicros(ts("2026-01-01T03:00:10.000000Z"));
                execute("INSERT INTO base VALUES ('2026-01-01T02:00:15.000000Z', 'a', 100)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 5
                                        && reader.getPartitionCount() == 2
                                        && reader.getMinTimestamp() == ts("2026-01-01T02:00:10.000000Z");
                            }
                        },
                        "the repair's replacement never evicted hour 01"
                );

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T02:00:10.000000Z\ta\t2.0
                                2026-01-01T02:00:15.000000Z\ta\t102.0
                                2026-01-01T02:00:20.000000Z\ta\t105.0
                                2026-01-01T02:00:30.000000Z\ta\t109.0
                                2026-01-01T03:00:10.000000Z\ta\t5.0
                                """);

                // The truncate route, not the splice: no root was re-versioned.
                Assert.assertEquals("the bound must decline the splice", 0, instance.getCheckpointRepairRootsVersioned());
                Assert.assertEquals(
                        "truncate, retention and seal must publish one generation each",
                        generationBefore + 3,
                        readGeneration(lvToken)
                );
                assertTimelineExists(lvToken, true);
                // The truncate retired the three roots at or above R, the retention the one
                // inside the evicted hour.
                Assert.assertEquals(retiredBefore + 4, readRetiredCheckpointCount(lvToken));
                assertLadder(
                        instance,
                        ts("2026-01-01T02:00:10.000000Z"), 1,
                        ts("2026-01-01T03:00:10.000000Z"), 5
                );
                Assert.assertEquals("the evicted row must leave the lifetime counter", 5, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            // The kept prefix and the fresh head are what a restart reads back, and the
            // evicted hour stays evicted.
            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T02:00:10.000000Z\t2.0
                            2026-01-01T02:00:15.000000Z\t102.0
                            2026-01-01T02:00:20.000000Z\t105.0
                            2026-01-01T02:00:30.000000Z\t109.0
                            2026-01-01T03:00:10.000000Z\t5.0
                            """);
            Assert.assertEquals(5, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testTtlEvictionInsideAnO3TruncateThatRetiredLeavesNoRetentionMarker() throws Exception {
        // The truncate route's other outcome: a correction below every root finds no prefix
        // to keep, so the truncate retires the timeline before the replay, and the
        // replacement's commit then evicts hour 01 - writing the retention marker after the
        // retire that would otherwise have removed it. There is no timeline for the marker
        // to guard and the post-replay seal opens a fresh history at the table's own size,
        // so the repair clears it. Without the change the marker outlives the repair and
        // the restart below rebuilds from the applied base instead of restoring the fresh
        // history.
        //
        // Two roots sit inside the correction's 30-second reach, so the bound of 1 declines
        // the capture; a correction reaching one root or none would keep it and take the
        // splice's own decline instead.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("ALTER LIVE VIEW lv SET TTL 1 HOUR");
                driveLiveViewWalApply(job);

                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T01:00:20.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 3, 3);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 4, 4);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                // Below every root, inside the hour the commit is about to evict: the
                // replacement writes the row and its own commit takes the partition.
                setCurrentMicros(ts("2026-01-01T03:00:10.000000Z"));
                execute("INSERT INTO base VALUES ('2026-01-01T01:00:05.000000Z', 'a', 100)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 2
                                        && reader.getPartitionCount() == 2
                                        && reader.getMinTimestamp() == ts("2026-01-01T02:00:10.000000Z");
                            }
                        },
                        "the repair's replacement never evicted hour 01"
                );

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T02:00:10.000000Z\ta\t3.0
                                2026-01-01T03:00:10.000000Z\ta\t4.0
                                """);

                Assert.assertEquals("the bound must decline the splice", 0, instance.getCheckpointRepairRootsVersioned());
                // A fresh history: the retire took the ladder, the seal opened a new one at
                // the frontier.
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("2026-01-01T03:00:10.000000Z"), 2);
                Assert.assertEquals(2, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T02:00:10.000000Z\t3.0
                            2026-01-01T03:00:10.000000Z\t4.0
                            """);
            Assert.assertEquals(2, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropQueuedBehindAParkedRepairLandsInItsSplice() throws Exception {
        // A DROP PARTITION sequenced while a localized out-of-order repair sits parked between its
        // turns - the one window in which a removal lands between a repair's capture and its
        // replacement's apply without racing a single turn. The parked session measured the view's
        // table before its replay and carries those counts across every turn, so the table must
        // not move until the repair's own final turn applies the replacement. That apply drains
        // the DROP first and the replacement behind it, and the splice publishes the removal in
        // the generation it commits: hour 01's root retires, every position above it drops by the
        // row that went, and the two repaired boundaries carry the replay's positions lowered by
        // the same range-add. It is the ladder a TTL eviction inside the replacement's own commit
        // produces; see testTtlEvictionInsideAnO3RepairSplicesRetentionIntoOneGeneration.
        //
        // A second worker's idle scan is what used to break this. Its apply retry saw a committed
        // transaction the view's table had not applied and landed the DROP under the parked
        // capture, publishing a retention over the generation the capture pinned. The repair's
        // final turn then failed its row-count proof - logged critical - and the exit path retired
        // the whole ladder, leaving the view with no timeline at all. Without the change this test
        // fails on the DROP having been applied while the repair is still parked.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createHourlyRangeView();
            final TableToken lvToken = engine.verifyTableName("lv");
            final LogCapture capture = new LogCapture();
            try (
                    LiveViewRefreshJob owner = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewRefreshJob foreign = new LiveViewRefreshJob(1, engine, 1)
            ) {
                final LiveViewInstance instance = flushHourlyRepairHistory(owner);
                final long generationBefore = readGeneration(lvToken);
                capture.start();

                // R is 02:00:15 and the 30-second frame converges one microsecond past 02:00:45,
                // so the repair re-versions 02:00:20 and 02:00:30. A budget of one base row per
                // turn parks it on its first turn.
                execute("INSERT INTO base VALUES ('2026-01-01T02:00:15.000000Z', 'a', 100)");
                driveUntilParked(owner, "lv");
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T01'");

                // Every worker's idle scan passes a view whose own WAL holds a transaction its
                // table has not applied. This worker does not own the parked repair, and it has
                // to leave the DROP to the one that does.
                for (int i = 0; i < 8; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    drainJob(foreign);
                    drainWalQueue();
                }
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);
                Assert.assertNotNull("the repair must still be parked", instance.getSuspendedRepair());
                Assert.assertEquals(
                        "the DROP must wait for the parked repair's own apply",
                        tracker.getSeqTxn() - 1,
                        tracker.getWriterTxn()
                );
                Assert.assertEquals("nothing may move the table under the parked capture", 5, lvRowCount(lvToken));
                Assert.assertEquals(generationBefore, readGeneration(lvToken));
                Assert.assertFalse(instance.hasPendingPartitionRemovals());

                driveParkedRepairToCompletion(owner, instance);
                driveRefreshToQuiescence(owner);
                capture.drain();
                capture.assertLogged("live view durable rows removed during an O3 repair [view=lv, removedRows=1, "
                        + "aheadOfReplacement=false, spliceable=true]");
                capture.assertLogged("live view O3 repair published its own retention [view=lv, removedRows=1, rootsRetired=1]");
                capture.assertNotLogged("live view has committed but unapplied WAL, retrying apply [view=lv");

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T02:00:10.000000Z\ta\t2.0
                                2026-01-01T02:00:15.000000Z\ta\t102.0
                                2026-01-01T02:00:20.000000Z\ta\t105.0
                                2026-01-01T02:00:30.000000Z\ta\t109.0
                                2026-01-01T03:00:10.000000Z\ta\t5.0
                                """);
                Assert.assertEquals(
                        "the repair and the removal it drained must publish exactly one generation",
                        generationBefore + 1,
                        readGeneration(lvToken)
                );
                Assert.assertEquals("the dropped hour's root, and only it", 1, readRetiredCheckpointCount(lvToken));
                assertLadder(
                        instance,
                        ts("2026-01-01T02:00:10.000000Z"), 1,
                        ts("2026-01-01T02:00:20.000000Z"), 3,
                        ts("2026-01-01T02:00:30.000000Z"), 4,
                        ts("2026-01-01T03:00:10.000000Z"), 5
                );
                Assert.assertEquals("the dropped row must leave the lifetime counter once", 5, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T02:00:10.000000Z\t2.0
                            2026-01-01T02:00:15.000000Z\t102.0
                            2026-01-01T02:00:20.000000Z\t105.0
                            2026-01-01T02:00:30.000000Z\t109.0
                            2026-01-01T03:00:10.000000Z\t5.0
                            """);
            Assert.assertEquals(5, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropInsideAParkedRepairsIntervalComesBackWithItsReplacement() throws Exception {
        // The same queued DROP, over the hour the parked repair is rewriting. The repair's final
        // apply drains the DROP first and the replacement behind it, so the removal takes all of
        // hour 02 and the replacement then re-emits the rows inside [R, H): 02:00:15, 02:00:20
        // and 02:00:30 come back off the base, while 02:00:10 - below R, outside anything the
        // replacement rewrites - stays dropped. That is the documented DROP PARTITION contract
        // with both of its halves inside one drain: the removal is durable now, and a recovery
        // that re-derives a period brings back exactly the rows it re-derives.
        //
        // What the ladder cannot keep is anything the two transactions touched together. The
        // repair measured the rows its replacement would delete before the DROP took them, so
        // its row-count proof counts those rows twice, and the removal's own correction would
        // lower both repaired roots by rows that are back on disk. The splice therefore declines
        // up front rather than failing that proof - which used to log a critical row-count
        // mismatch for an ordinary operator action - and the timeline retires ahead of the
        // post-replay seal, so the seal opens a fresh history at the frontier. The retire used to
        // run after the seal and take that head with it: the view kept no timeline until its next
        // cadence seal, and a restart in between rebuilt it from the applied base, which brings
        // 02:00:10 back. The restart below restores instead, and 02:00:10 stays dropped.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createHourlyRangeView();
            final TableToken lvToken = engine.verifyTableName("lv");
            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = flushHourlyRepairHistory(job);
                capture.start();

                execute("INSERT INTO base VALUES ('2026-01-01T02:00:15.000000Z', 'a', 100)");
                driveUntilParked(job, "lv");
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T02'");
                driveParkedRepairToCompletion(job, instance);
                driveRefreshToQuiescence(job);
                capture.drain();
                capture.assertNotLogged("replacement row count does not match the repair plan");
                // The retire ahead of the seal takes the repair directory with it, so the
                // descriptor the publication mirrors its stages into has to go too; left open,
                // its next stage fails to rewrite it and logs critical.
                capture.assertNotLogged("could not update a live view checkpoint repair descriptor");
                capture.assertLogged("live view durable rows removed during an O3 repair [view=lv, removedRows=3, "
                        + "aheadOfReplacement=true, spliceable=false]");

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T01:00:10.000000Z\ta\t1.0
                                2026-01-01T02:00:15.000000Z\ta\t102.0
                                2026-01-01T02:00:20.000000Z\ta\t105.0
                                2026-01-01T02:00:30.000000Z\ta\t109.0
                                2026-01-01T03:00:10.000000Z\ta\t5.0
                                """);
                // A fresh history opened at the frontier by the post-replay seal, stamped at the
                // count the table holds.
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("2026-01-01T03:00:10.000000Z"), 5);
                Assert.assertEquals(0, readRetiredCheckpointCount(lvToken));
                Assert.assertEquals("the removal must leave the lifetime counter once", 5, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T01:00:10.000000Z\t1.0
                            2026-01-01T02:00:15.000000Z\t102.0
                            2026-01-01T02:00:20.000000Z\t105.0
                            2026-01-01T02:00:30.000000Z\t109.0
                            2026-01-01T03:00:10.000000Z\t5.0
                            """);
            Assert.assertEquals(5, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropAheadOfAResumeReplacementIsCountedByTheTable() throws Exception {
        // The resume executor's side of the same shape, which no parked session reaches: it never
        // yields, so a DROP PARTITION lands between its capture and its apply only by being
        // sequenced while its single turn replays, and the hook stands in for that second session.
        // The correction at 02:59:55 sits within one frame width of the 03:00:05 frontier, so
        // localizing would read to the end of the base anyway and the plan resumes from the anchor
        // at 02:59:50. The frontier is in hour 03, which leaves hour 02 - the anchor's own
        // partition - droppable. The DROP takes all of hour 02 and the resume's replacement then
        // re-emits everything above the anchor: 02:59:55, 02:59:58 and 03:00:05 come back, and
        // 02:59:30 to 02:59:50 stay dropped.
        //
        // What went wrong here was the counter. The resume carries it forward from the anchor
        // rather than re-seating it from the table, and its declined splice left the removal
        // pending on the premise that the removal's rows are missing from the table - true of a
        // TTL eviction in the replacement's own commit, false of a removal the replacement then
        // re-emitted. The seal's reconcile subtracted every dropped row from a counter that already
        // matched the table, logged a critical row-count mismatch, retired the timeline and counted
        // a checkpoint_row_count_mismatches. The resume now takes the counter from the table and
        // drops the events, and the seal opens a fresh history at that count.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createHourlyRangeView();
            final TableToken lvToken = engine.verifyTableName("lv");
            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:59:30.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T02:59:40.000000Z", 3, 3);
                flushOneRow(job, "2026-01-01T02:59:50.000000Z", 4, 4);
                flushOneRow(job, "2026-01-01T02:59:58.000000Z", 5, 5);
                flushOneRow(job, "2026-01-01T03:00:05.000000Z", 6, 6);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                capture.start();

                final AtomicBoolean dropSequenced = new AtomicBoolean();
                job.setSimulateDdlBeforeRepairReplacementForTest(() -> {
                    try {
                        execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T02'");
                        dropSequenced.set(true);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                });
                execute("INSERT INTO base VALUES ('2026-01-01T02:59:55.000000Z', 'a', 100)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return dropSequenced.get() && reader.size() == 4;
                            }
                        },
                        "the resume never landed its replacement behind the DROP"
                );
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the resume, not a localized rebuild, must have run", 3, instance.getO3ResumeReplayRows());
                capture.drain();
                capture.assertNotLogged("row count does not match");
                capture.assertLogged("live view removal landed ahead of the O3 resume replacement, retiring instead of "
                        + "splicing [view=lv, removedRows=4, ");

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T01:00:10.000000Z\ta\t1.0
                                2026-01-01T02:59:55.000000Z\ta\t109.0
                                2026-01-01T02:59:58.000000Z\ta\t114.0
                                2026-01-01T03:00:05.000000Z\ta\t118.0
                                """);
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("2026-01-01T03:00:05.000000Z"), 4);
                Assert.assertEquals("the counter must hold the table's own count", 4, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T01:00:10.000000Z\t1.0
                            2026-01-01T02:59:55.000000Z\t109.0
                            2026-01-01T02:59:58.000000Z\t114.0
                            2026-01-01T03:00:05.000000Z\t118.0
                            """);
            Assert.assertEquals(4, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropQueuedBehindADeferredReplacementIsReconciledOnce() throws Exception {
        // The other order inside one drain. The repair's replacement commits but its apply stalls,
        // so the repair defers - its timeline retired, the base range left unconsumed - and a DROP
        // of the hour the replacement rewrote is sequenced behind it. The next apply lands the
        // replacement and then the DROP, which takes the rows the replacement had just written;
        // the deferred repair then runs again from the same out-of-order row and re-emits [R, H)
        // a second time. The removal is counted once: the apply that lands it subtracts it from
        // the counter, the repeated repair re-seats that counter from the table rather than
        // subtracting again, and nothing seals in between to record a mismatch.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createHourlyRangeView();
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = flushHourlyRepairHistory(job);

                job.setSimulateRepairApplyFailureForTest(true);
                execute("INSERT INTO base VALUES ('2026-01-01T02:00:15.000000Z', 'a', 100)");
                drainWalQueue();
                // Exactly one pass: it drains the notification and runs the repair, so the idle
                // scan's apply retry does not land the stalled replacement before the DROP is in.
                Assert.assertTrue(job.run());
                drainWalQueue();
                final long replacementLvSeqTxn = instance.getPendingReplacementLvSeqTxn();
                Assert.assertNotEquals("the replacement must be left for reconciliation", Numbers.LONG_NULL, replacementLvSeqTxn);
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);
                Assert.assertEquals(replacementLvSeqTxn - 1, tracker.getWriterTxn());
                assertTimelineExists(lvToken, false);
                Assert.assertEquals(5, instance.getLvRowsTotal());

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T02'");
                Assert.assertEquals("the DROP must sit behind the replacement", replacementLvSeqTxn + 1, tracker.getSeqTxn());
                job.setSimulateRepairApplyFailureForTest(false);
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);

                // Hour 02 went whole, the replacement's rows with it, and the repeated repair
                // brought [R, H) back; 02:00:10 sits below R and stays dropped.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T01:00:10.000000Z\ta\t1.0
                                2026-01-01T02:00:15.000000Z\ta\t102.0
                                2026-01-01T02:00:20.000000Z\ta\t105.0
                                2026-01-01T02:00:30.000000Z\ta\t109.0
                                2026-01-01T03:00:10.000000Z\ta\t5.0
                                """);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getPendingReplacementLvSeqTxn());
                assertLadder(instance, ts("2026-01-01T03:00:10.000000Z"), 5);
                Assert.assertEquals("the removal must leave the lifetime counter once", 5, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT ts, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T01:00:10.000000Z\t1.0
                            2026-01-01T02:00:15.000000Z\t102.0
                            2026-01-01T02:00:20.000000Z\t105.0
                            2026-01-01T02:00:30.000000Z\t109.0
                            2026-01-01T03:00:10.000000Z\t5.0
                            """);
            Assert.assertEquals(5, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testDropAheadOfAKeyedResumesUpsertComesBackOnlyForTheKeysItFollows() throws Exception {
        // The resume executor's keyed route, which publishes an upsert of the rows it recomputed
        // rather than a replacement of everything above its anchor. The correction at 02:35 names
        // acct-1 alone, so the resume follows acct-1's rows above the anchor at 01:40 and leaves
        // every other account's stored rows where they stand. The hook sequences a DROP of hour 02
        // right before that upsert commits, so one apply drains the removal first and the upsert
        // behind it.
        //
        // That is where this route parts from the replacement: the upsert re-inserts the rows it
        // recomputed, and only those. acct-1's 02:10 comes back beside the correction; acct-2, acct-3
        // and acct-4 lose their hour-02 rows for good, because nothing this repair publishes names
        // them. Both halves are the documented DROP PARTITION contract - a recovery that re-derives
        // output brings back exactly what it re-derives - and a keyed repair re-derives by key.
        //
        // The ladder cannot keep what the two transactions touched together, for the reason the
        // replacement cannot: the removal took a row the upsert then re-inserted. So the resume
        // declines the splice up front, takes its lifetime counter from the table and opens a fresh
        // history at the frontier, exactly as testDropAheadOfAResumeReplacementIsCountedByTheTable
        // pins for the whole-range route.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createKeyedView();
            final TableToken lvToken = engine.verifyTableName("lv");
            // What the DROP takes for good: every other account's row of the hour, since nothing
            // the repair publishes names them.
            final String droppedForGood = "created_at IN '2026-01-04T02' AND account_id <> 'acct-1'";
            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openKeyedDayAboveRoots(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(120, lvRowCount(lvToken));
                capture.start();

                final AtomicBoolean dropSequenced = new AtomicBoolean();
                job.setSimulateDdlBeforeRepairReplacementForTest(() -> {
                    try {
                        execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-04T02'");
                        dropSequenced.set(true);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                });
                commitKeyed(keyedRow(4, 2, 35, "acct-1"), job);
                Assert.assertTrue("the hook must have sequenced the DROP", dropSequenced.get());
                Assert.assertEquals("the resume must follow the correction's own key", 1, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals("and publish the upsert rather than fall back", 1, job.openSegmentSparseResumeCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                capture.drain();
                capture.assertNotLogged("row count does not match");
                capture.assertLogged("live view removal landed ahead of the O3 resume replacement, retiring instead of "
                        + "splicing [view=lv, removedRows=4, ");

                // Hour 02 holds acct-1's two rows and nothing else: 02:10 re-inserted by the upsert,
                // 02:35 the correction itself.
                assertQuery("SELECT created_at, account_id, cumulative_sum FROM lv WHERE created_at IN '2026-01-04T02'")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .returns("""
                                created_at\taccount_id\tcumulative_sum
                                2026-01-04T02:10:00.000000Z\tacct-1\t3.0
                                2026-01-04T02:35:00.000000Z\tacct-1\t4.0
                                """);
                assertKeyedViewMatchesRecomputeExcept(droppedForGood);
                Assert.assertEquals(118, lvRowCount(lvToken));
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("2026-01-04T09:40:00.000000Z"), 118);
                Assert.assertEquals("the counter must hold the table's own count", 118, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            Assert.assertEquals(118, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The restart restored the root the post-replay seal imaged once the transplant had
                // handed acct-1's corrected accumulators back, so a forward row of each key has to
                // fold onto the right sums.
                commitKeyed(keyedRow(4, 10, 10, "acct-1") + ", " + keyedRow(4, 10, 20, "acct-2"), job);
                assertKeyedViewMatchesRecomputeExcept(droppedForGood);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDropAheadOfAKeyedResumesUpsertBelowItsAnchorLandsInItsSplice() throws Exception {
        // The same hook, over the hour below the anchor's own partition. The removal is still ordered
        // ahead of the upsert, but it lies wholly below the range the upsert rewrites, so it took no
        // row the upsert re-inserts and the splice can carry it: hour 00's root retires, and the
        // removal's range-add at the anchor lowers every root above it - the ones this resume
        // re-versioned included - by the four rows that went.
        //
        // The keyed route is the one that makes this worth a case of its own. It does not rewind its
        // lifetime counter to the anchor the way the whole-range resume does; it carries the
        // pre-repair table size forward and advances it by the exact insert delta at the head seal,
        // and its row-count proof is that same arithmetic. So a published splice has to take the
        // removed rows off a counter that still counts them, once, before the seal stamps the head.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createKeyedView();
            final TableToken lvToken = engine.verifyTableName("lv");
            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openKeyedDayAboveRoots(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final long generationBefore = readGeneration(lvToken);
                capture.start();

                final AtomicBoolean dropSequenced = new AtomicBoolean();
                job.setSimulateDdlBeforeRepairReplacementForTest(() -> {
                    try {
                        execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-04T00'");
                        dropSequenced.set(true);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                });
                commitKeyed(keyedRow(4, 2, 35, "acct-1"), job);
                Assert.assertTrue("the hook must have sequenced the DROP", dropSequenced.get());
                Assert.assertEquals(1, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(1, job.openSegmentSparseResumeCountForTest());
                capture.drain();
                capture.assertNotLogged("row count does not match");
                capture.assertNotLogged("live view removal landed ahead of the O3 resume replacement");
                capture.assertLogged("live view O3 resume repair published its own retention [view=lv, removedRows=4, rootsRetired=1]");

                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-04T00'");
                Assert.assertEquals(117, lvRowCount(lvToken));
                Assert.assertEquals(
                        "the upsert and the removal it drained must publish exactly one generation",
                        generationBefore + 1,
                        readGeneration(lvToken)
                );
                Assert.assertEquals("hour 00's root, and only it", 1, readRetiredCheckpointCount(lvToken));
                assertLadder(
                        instance,
                        ts("2026-01-03T09:40:00.000000Z"), 80,
                        ts("2026-01-04T01:40:00.000000Z"), 84,
                        ts("2026-01-04T02:40:00.000000Z"), 89,
                        ts("2026-01-04T03:40:00.000000Z"), 93,
                        ts("2026-01-04T04:40:00.000000Z"), 97,
                        ts("2026-01-04T05:40:00.000000Z"), 101,
                        ts("2026-01-04T06:40:00.000000Z"), 105,
                        ts("2026-01-04T07:40:00.000000Z"), 109,
                        ts("2026-01-04T08:40:00.000000Z"), 113,
                        ts("2026-01-04T09:40:00.000000Z"), 117
                );
                assertKeyedLadderCountsRowsAtOrBelowEachBoundary(instance);
                Assert.assertEquals("the dropped rows must leave the lifetime counter once", 117, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            Assert.assertEquals(117, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitKeyed(keyedRow(4, 10, 10, "acct-1") + ", " + keyedRow(4, 10, 20, "acct-2"), job);
                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-04T00'");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDropQueuedBehindAParkedColdKeyedRepairComesBackOnlyForTheKeysItFollows() throws Exception {
        // The head-miss executor's keyed route. The seed leaves one root, at 2026-01-03T09:40, and the
        // correction at 02:05 sits below it, so there is no anchor to resume from: the repair replays
        // acct-1 cold from the day's origin, emits from the correction up, and publishes the rows it
        // recomputed as an upsert. A one-row turn budget parks it on its first turn, and a DROP of
        // hour 02 is sequenced while it sits parked - the shape a user's DROP takes against a repair
        // that spans turns.
        //
        // The parked session measured the table before its replay, so the DROP waits for the
        // repair's own final apply, which drains it ahead of the upsert. The upsert then re-inserts
        // acct-1's 02:10 - above the emit floor, so recomputed - beside the correction, and nothing
        // else: the other accounts' hour-02 rows stay dropped, exactly as
        // testDropAheadOfAKeyedResumesUpsertComesBackOnlyForTheKeysItFollows has it for the resume
        // executor. The re-inserted row is what the ladder cannot account for: the upsert's proof
        // counts 02:10 as a row it replaces, and the removal's range-add would lower the root above
        // it by a row that is back on disk. So the splice declines on the same rule and the timeline
        // retires ahead of the post-replay seal, which opens a fresh history at the frontier.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createKeyedView();
            final TableToken lvToken = engine.verifyTableName("lv");
            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(instance, ts("2026-01-03T09:40:00.000000Z"), 80);
                capture.start();

                execute("INSERT INTO tx VALUES " + keyedRow(3, 2, 5, "acct-1"));
                driveUntilParked(job, "lv");
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-03T02'");
                driveParkedRepairToCompletion(job, instance);
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);
                Assert.assertEquals("the repair's own apply must have landed the DROP", tracker.getSeqTxn(), tracker.getWriterTxn());
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the repair must replay cold by key", 1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals("and publish the upsert rather than fall back", 1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                capture.drain();
                capture.assertNotLogged("row count does not match");
                capture.assertNotLogged("could not update a live view checkpoint repair descriptor");
                capture.assertLogged("live view keyed repair published sparsely [view=lv, origin=segment start, ");
                capture.assertLogged("outputLowTs=2026-01-03T02:05:00.000000Z");
                capture.assertLogged("live view durable rows removed during an O3 repair [view=lv, removedRows=4, "
                        + "aheadOfReplacement=true, spliceable=false]");

                assertQuery("SELECT created_at, account_id, cumulative_sum FROM lv WHERE created_at IN '2026-01-03T02'")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .returns("""
                                created_at\taccount_id\tcumulative_sum
                                2026-01-03T02:05:00.000000Z\tacct-1\t3.0
                                2026-01-03T02:10:00.000000Z\tacct-1\t4.0
                                """);
                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-03T02' AND account_id <> 'acct-1'");
                Assert.assertEquals(78, lvRowCount(lvToken));
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("2026-01-03T09:40:00.000000Z"), 78);
                Assert.assertEquals("the counter must hold the table's own count", 78, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            Assert.assertEquals(78, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitKeyed(keyedRow(3, 10, 10, "acct-1") + ", " + keyedRow(3, 10, 20, "acct-2"), job);
                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-03T02' AND account_id <> 'acct-1'");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDropAheadOfAClosedSegmentKeyedUpsertComesBackOnlyForTheKeysItFollows() throws Exception {
        // The head-miss executor's other keyed publisher: a correction inside a segment the runtime
        // has already left. The daily anchor closes 2026-01-02 once the view stands on 2026-01-03, so
        // the correction at 02:05 is repaired per segment, by key, and published as an upsert over
        // that day alone. Its row-count proof is not the cold route's insert delta but the rows the
        // segment held, less the ones it replaced, plus the ones it wrote and the ones its merge
        // left standing - a different formula over the same removal.
        //
        // The hook sequences a DROP of the hour being corrected right before the upsert commits. The
        // outcome is the one the other two keyed routes have: acct-1's 02:10 comes back beside the
        // correction, every other account's hour-02 row stays dropped, and the ladder retires ahead
        // of a fresh seal at the frontier because the upsert re-inserted a row the removal took.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createKeyedView();
            final TableToken lvToken = engine.verifyTableName("lv");
            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                capture.start();

                final AtomicBoolean dropSequenced = new AtomicBoolean();
                job.setSimulateDdlBeforeRepairReplacementForTest(() -> {
                    try {
                        execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-02T02'");
                        dropSequenced.set(true);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                });
                commitKeyed(keyedRow(2, 2, 5, "acct-1"), job);
                Assert.assertTrue("the hook must have sequenced the DROP", dropSequenced.get());
                Assert.assertEquals("the closed segment must be repaired by key", 1, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals("and publish the upsert rather than fall back", 1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                capture.drain();
                capture.assertNotLogged("row count does not match");
                capture.assertNotLogged("could not update a live view checkpoint repair descriptor");
                capture.assertLogged("live view keyed repair published sparsely [view=lv, origin=closed segment, ");
                capture.assertLogged("live view durable rows removed during an O3 repair [view=lv, removedRows=4, "
                        + "aheadOfReplacement=true, spliceable=false]");

                assertQuery("SELECT created_at, account_id, cumulative_sum FROM lv WHERE created_at IN '2026-01-02T02'")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .returns("""
                                created_at\taccount_id\tcumulative_sum
                                2026-01-02T02:05:00.000000Z\tacct-1\t3.0
                                2026-01-02T02:10:00.000000Z\tacct-1\t4.0
                                """);
                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-02T02' AND account_id <> 'acct-1'");
                Assert.assertEquals(78, lvRowCount(lvToken));
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("2026-01-03T09:40:00.000000Z"), 78);
                Assert.assertEquals("the counter must hold the table's own count", 78, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            Assert.assertEquals(78, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitKeyed(keyedRow(3, 10, 10, "acct-1") + ", " + keyedRow(3, 10, 20, "acct-2"), job);
                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-02T02' AND account_id <> 'acct-1'");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDropQueuedBehindAParkedColdKeyedRepairBelowItsFloorLandsInItsSplice() throws Exception {
        // The same parked cold repair with the DROP a day below it: hour 05 of 2026-01-02, a segment
        // the daily anchor keeps out of everything a correction on 2026-01-03 re-derives. The removal
        // is still drained ahead of the upsert, but it took no row the upsert re-inserts, so the
        // splice carries it in the generation it commits.
        //
        // What this pins is the sparse route's own row-count proof. A replacement proves itself
        // against the rows it deleted and wrote; an upsert deletes nothing, so its proof is the
        // pre-repair count plus the exact insert delta, and the removal has to come off that once.
        // The seed's single root is the one boundary the repair re-versions, and the removal's
        // range-add below it lowers the position the replay derived for it by the four rows that
        // went.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createKeyedView();
            final TableToken lvToken = engine.verifyTableName("lv");
            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                final long generationBefore = readGeneration(lvToken);
                capture.start();

                execute("INSERT INTO tx VALUES " + keyedRow(3, 2, 35, "acct-1"));
                driveUntilParked(job, "lv");
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-02T05'");
                driveParkedRepairToCompletion(job, instance);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                capture.drain();
                capture.assertNotLogged("row count does not match");
                capture.assertLogged("live view durable rows removed during an O3 repair [view=lv, removedRows=4, "
                        + "aheadOfReplacement=false, spliceable=true]");
                capture.assertLogged("live view O3 repair published its own retention [view=lv, removedRows=4, rootsRetired=0]");

                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-02T05'");
                Assert.assertEquals(77, lvRowCount(lvToken));
                Assert.assertEquals(
                        "the upsert and the removal it drained must publish exactly one generation",
                        generationBefore + 1,
                        readGeneration(lvToken)
                );
                Assert.assertEquals("no root sat inside the dropped hour", 0, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("2026-01-03T09:40:00.000000Z"), 77);
                assertKeyedLadderCountsRowsAtOrBelowEachBoundary(instance);
                Assert.assertEquals("the dropped rows must leave the lifetime counter once", 77, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRepairMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            Assert.assertEquals(77, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitKeyed(keyedRow(3, 10, 10, "acct-1") + ", " + keyedRow(3, 10, 20, "acct-2"), job);
                assertKeyedViewMatchesRecomputeExcept("created_at IN '2026-01-02T05'");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTtlEvictionInsideDedupCleanCycleRebuildsTier() throws Exception {
        // A view over a DEDUP base is coupled: it has no un-flushed lead, applies inline every
        // cycle, and its disk-subset publish is the tier's only feed. When the range is provably
        // clean the cycle runs through incrementalRefresh's raw-WAL drain, and when that cycle's
        // own apply evicts a TTL partition it un-stamps the slot. It has to rebuild it in the same
        // cycle: there is no flush to come back for, and refreshInstance only runs again on a new
        // base commit, so an idle view would otherwise read disk-only for good.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDedupBaseAndView();
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1, 'a'), " +
                    "('1970-01-02T00:00:00.000000Z', 2, 'b')");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 2);
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals("a dedup base is coupled and carries no lead", 0, instance.getLeadRowCount());
                assertLadder(instance, ts("1970-01-02"), 2);

                execute("ALTER LIVE VIEW lv SET TTL 1 DAY");
                driveLiveViewWalApply(job);
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                Assert.assertEquals(2, instance.getLvRowsTotal());

                // Nothing in the range dedups, so isRangeProvablyClean admits it and the cycle
                // takes the raw-WAL drain. TTL judges age by the smaller of the table frontier and
                // the wall clock, so move the clock up to the frontier this commit creates.
                final long cleanCyclesBefore = instance.getDedupRawWalCleanCycles();
                setCurrentMicros(ts("1970-01-03T00:00:00.000000Z"));
                execute("INSERT INTO base VALUES ('1970-01-03T00:00:00.000000Z', 3, 'c')");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 2 && reader.getPartitionCount() == 2 && reader.getMinTimestamp() == ts("1970-01-02");
                            }
                        },
                        "the dedup-clean cycle never evicted 1970-01-01"
                );
                Assert.assertTrue(
                        "the evicting cycle must take the clean raw-WAL drain, not drainAppliedBase",
                        instance.getDedupRawWalCleanCycles() > cleanCyclesBefore
                );
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                """);
                // The cycle un-stamped the slot and rebuilt it from the surviving table before it
                // returned: no base commit has arrived since, and a fresh cursor seams again.
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                Assert.assertFalse("the cycle must rebuild the tier itself", instance.isTierStale());
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
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(0, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-02"), 1, ts("1970-01-03"), 2);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn
                            1970-01-02T00:00:00.000000Z\t2\t1
                            1970-01-03T00:00:00.000000Z\t3\t1
                            """);
        });
    }

    @Test
    public void testTtlEvictionInsideDedupForwardAppendRebuildsTier() throws Exception {
        // The other coupled cycle of a DEDUP base: a range that actually deduped is not provably
        // clean, so the refresh reads the applied, post-dedup base through drainAppliedBase. Its
        // forward append un-stamps the slot on an eviction exactly as the clean cycle does, and
        // owes the rebuild in that same cycle for the same reason.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDedupBaseAndView();
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1, 'a'), " +
                    "('1970-01-02T00:00:00.000000Z', 2, 'b')");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveUntilDurableRowCount(job, 2);
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(instance, ts("1970-01-02"), 2);

                execute("ALTER LIVE VIEW lv SET TTL 1 DAY");
                driveLiveViewWalApply(job);
                Assert.assertFalse(instance.hasPendingPartitionRemovals());

                // Both rows of this commit share (ts, sym), so the apply dedups one away and the
                // base's clean-range signal diverges - which routes the refresh through
                // drainAppliedBase. The commit's minimum timestamp is still above the view's
                // frontier, so it takes the forward append there rather than an O3 replay.
                final long cleanCyclesBefore = instance.getDedupRawWalCleanCycles();
                setCurrentMicros(ts("1970-01-03T00:00:00.000000Z"));
                execute("INSERT INTO base VALUES " +
                        "('1970-01-03T00:00:00.000000Z', 3, 'c'), " +
                        "('1970-01-03T00:00:00.000000Z', 33, 'c')");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 2 && reader.getPartitionCount() == 2 && reader.getMinTimestamp() == ts("1970-01-02");
                            }
                        },
                        "the dedup forward append never evicted 1970-01-01"
                );
                Assert.assertEquals(
                        "the evicting cycle must take drainAppliedBase, not the clean raw-WAL drain",
                        cleanCyclesBefore,
                        instance.getDedupRawWalCleanCycles()
                );
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                """);
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                Assert.assertFalse("the cycle must rebuild the tier itself", instance.isTierStale());
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                // The deduped row is the survivor of the pair, so x = 33 rather than 3.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-02T00:00:00.000000Z\t2\t1
                                1970-01-03T00:00:00.000000Z\t33\t1
                                """);
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                Assert.assertEquals("the evicted row must leave the lifetime counter", 2, instance.getLvRowsTotal());
                assertRetentionMarker(lvToken, false);
                assertTimelineExists(lvToken, true);
                Assert.assertEquals(0, readRetiredCheckpointCount(lvToken));
                assertLadder(instance, ts("1970-01-02"), 1, ts("1970-01-03"), 2);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn
                            1970-01-02T00:00:00.000000Z\t2\t1
                            1970-01-03T00:00:00.000000Z\t33\t1
                            """);
        });
    }

    @Test
    public void testTtlEvictionIntoTheOverlapBandRebuildsTheBandItInvalidates() throws Exception {
        // The eviction the seam arithmetic has to survive: an IN MEMORY window wider than what the
        // TTL keeps, so the rows the flush's own commit evicts are rows the overlap band holds. The
        // band is the identity the seam cuts on - it has to BE the LV table's trailing rows - and an
        // evicted prefix breaks it. Re-stamping the slot does not lose a row, because the read path
        // treats a band longer than the disk scan as a contract violation and degrades it to
        // lead-only (LiveViewRecordCursor asserts leadStart <= diskSize under -ea and falls back
        // otherwise), but that band is now one row too long against every later disk size: the seam
        // never cuts again on a view where nothing else forces a rebuild, and RAM holds rows no
        // reader can be served from. The flush un-stamps and rebuilds instead, and the band comes
        // back as the surviving table's own suffix in the same cycle - with the assert as the thing
        // that fires first if it ever stops doing so.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // The default cap on the IN MEMORY window is an hour, which is exactly the span an hour
        // TTL keeps: the band could never reach a row the eviction takes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_MAX, "3h");
        assertMemoryLeak(() -> {
            createHourlyBandedView("3h", "TTL 1 HOUR ");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "1970-01-01T00:00:00.000000Z", 1, 1);
                flushOneRow(job, "1970-01-01T01:00:00.000000Z", 2, 2);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                // Both hours sit inside the 3h window, so the band is the whole table and the hour
                // TTL is about to take a row out from under it.
                assertBand(instance, "1970-01-01T00:00:00.000000Z", 2);
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);

                // The flush that lands 02:00 evicts hour 00 inside its own commit. TTL judges age by
                // the smaller of the table frontier and the wall clock, and the clock was pinned
                // below the data, so move it up to the frontier the flush is about to reach.
                setCurrentMicros(ts("1970-01-01T02:00:00.000000Z"));
                execute("INSERT INTO base VALUES ('1970-01-01T02:00:00.000000Z', 'a', 3)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 2 && reader.getMinTimestamp() == ts("1970-01-01T01:00:00.000000Z");
                            }
                        },
                        "the flush never evicted hour 00"
                );
                driveRefreshToQuiescence(job);

                assertQuery("SELECT ts, x, rn FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-01T01:00:00.000000Z\t2\t2
                                1970-01-01T02:00:00.000000Z\t3\t3
                                """);
                assertQuery("SELECT count(), max(rn) FROM lv")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\tmax\n2\t3\n");
                // The band is the surviving table's suffix: the evicted row is not resident.
                assertBand(instance, "1970-01-01T01:00:00.000000Z", 2);
                Assert.assertFalse("the flush must rebuild the band it invalidated", instance.isTierStale());
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                Assert.assertEquals(2, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);

                // A row inside the surviving hour, so disk grows under the band without another
                // eviction - which is where a band left one row too long stops cutting for good.
                execute("INSERT INTO base VALUES ('1970-01-01T02:30:00.000000Z', 'a', 4)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                assertBand(instance, "1970-01-01T01:00:00.000000Z", 3);
                assertQuery("SELECT ts, x, rn FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-01T01:00:00.000000Z\t2\t2
                                1970-01-01T02:00:00.000000Z\t3\t3
                                1970-01-01T02:30:00.000000Z\t4\t4
                                """);
                assertQuery("SELECT count(), max(rn) FROM lv")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\tmax\n3\t4\n");
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTtlEvictionUnderAPinnedCursorKeepsThatSnapshotWhole() throws Exception {
        // Section 2.2's cached-row clause, in the shape a reader meets it: a cursor open across the
        // evicting flush pinned its slot and its disk reader before the removal, so it goes on
        // serving rows the table no longer holds. What it may not do is disagree with itself - the
        // size() a LIMIT reads and the rows the scan yields are one snapshot - or lose a row it was
        // already serving. Meanwhile a cursor opened after the removal sees the surviving table, so
        // both snapshots stand at once and neither borrows the other's rows.
        //
        // The pin is what buys that: the flush's rebuild replaces the published slot IN PLACE when
        // no reader holds it, and takes the other slot only because this cursor does. A rebuild
        // that read the pin wrongly would rewrite the rows under an open scan.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // The default cap on the IN MEMORY window is an hour, which is exactly the span an hour
        // TTL keeps: the band could never reach a row the eviction takes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_MAX, "3h");
        assertMemoryLeak(() -> {
            createHourlyBandedView("3h", "TTL 1 HOUR ");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "1970-01-01T00:00:00.000000Z", 1, 1);
                flushOneRow(job, "1970-01-01T01:00:00.000000Z", 2, 2);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                try (
                        RecordCursorFactory factory = select("SELECT ts, x, rn FROM lv");
                        LiveViewRecordCursor pinned = (LiveViewRecordCursor) unwrapLvFactory(factory).getCursor(sqlExecutionContext)
                ) {
                    // The band holds both rows and disk holds both rows, so the seam cuts the disk
                    // scan at zero and the pinned snapshot lives entirely in the slot.
                    Assert.assertEquals(LiveViewRecordCursor.ROUTING_SEAM, pinned.routingMode());
                    Assert.assertEquals("[1,2]", drainPinnedCursor(pinned).toString());

                    setCurrentMicros(ts("1970-01-01T02:00:00.000000Z"));
                    execute("INSERT INTO base VALUES ('1970-01-01T02:00:00.000000Z', 'a', 3)");
                    driveUntil(
                            job,
                            () -> {
                                try (TableReader reader = engine.getReader(lvToken)) {
                                    return reader.size() == 2 && reader.getMinTimestamp() == ts("1970-01-01T01:00:00.000000Z");
                                }
                            },
                            "the flush never evicted hour 00"
                    );

                    // The pinned snapshot is unchanged: the evicted row is still served, the row the
                    // flush added is not, and size() still names the rows the scan yields.
                    Assert.assertEquals("[1,2]", drainPinnedCursor(pinned).toString());
                    // A cursor opened now reads the surviving table instead.
                    assertQuery("SELECT ts, x, rn FROM lv")
                            .noLeakCheck()
                            .timestamp("ts")
                            .expectSize()
                            .returns("""
                                    ts\tx\trn
                                    1970-01-01T01:00:00.000000Z\t2\t2
                                    1970-01-01T02:00:00.000000Z\t3\t3
                                    """);
                }

                driveRefreshToQuiescence(job);
                assertBand(instance, "1970-01-01T01:00:00.000000Z", 2);
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                Assert.assertEquals(2, instance.getLvRowsTotal());
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testSetTtlZeroStopsEvictionWithoutBringingBackWhatWentEarlier() throws Exception {
        // The clear, on an ACTIVE view that has already lost a day to the TTL it clears. Two things
        // have to hold and they pull in opposite directions: no partition may be evicted after the
        // clear - 1970-01-02 goes on the next flush while the TTL stands, and stays while it does
        // not - and the day the earlier TTL took may not come back, because a removal is durable and
        // clearing the retention that caused it is not a restore.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                flushRow(job, "1970-01-02", 2, 2);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                execute("ALTER LIVE VIEW lv SET TTL 1 DAY");
                driveLiveViewWalApply(job);
                setCurrentMicros(ts("1970-01-03T00:00:00.000000Z"));
                execute("INSERT INTO base VALUES ('1970-01-03T00:00:00.000000Z', 3)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 2 && reader.getMinTimestamp() == ts("1970-01-02");
                            }
                        },
                        "the flush never evicted 1970-01-01"
                );
                driveRefreshToQuiescence(job);
                Assert.assertEquals(2, instance.getLvRowsTotal());

                execute("ALTER LIVE VIEW lv SET TTL 0 HOURS");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);
                assertTtl("lv", 0);

                // Under TTL 1 DAY this flush would take 1970-01-02 with it: its ceiling is
                // 1970-01-03 and the frontier reaches a day past that.
                setCurrentMicros(ts("1970-01-04T00:00:00.000000Z"));
                execute("INSERT INTO base VALUES ('1970-01-04T00:00:00.000000Z', 4)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);

                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-02
                                1970-01-03
                                1970-01-04
                                """);
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
                Assert.assertEquals("the cleared TTL does not restore the evicted day", 3, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            // The restart restores from the timeline the eviction and the clear both left behind,
            // and 1970-01-01 is still gone.
            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n3\n4\n");
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testClearingTtlMidSeedStopsEvictionWithoutRestoringTheSweepsOrdinal() throws Exception {
        // The clear against a SEEDING view: it has to stop the next eviction - 1970-01-02 and
        // 1970-01-03 go with the sweep's next two commits while the TTL stands - without bringing
        // back the day the TTL already took or disturbing what the sweep has written. lvRowsTotal
        // is the EMITTED output total for the whole sweep, so it leads the table by the evicted row
        // until the completion boundary corrects it once, and rn carries each row's position in the
        // stream the window ran over, which is what a re-emitted or recomputed row would break.
        // The counterexample for the deferral itself - what a reconcile that ran mid-sweep would
        // cost a resume - is testRestartAfterClearingTtlMidSeedStillReplacesUnprovenOutput; a sweep
        // that runs to completion in one process has no skip-write floor to get wrong.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        assertMemoryLeak(() -> {
            createSevenDaySeedBase();
            createSeedView("TTL 2 DAYS ");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = driveSeedTurnsUntilEviction(job);
                // The sweep's own commit of 1970-01-04 evicted 1970-01-01: its ceiling is
                // 1970-01-02 and the sweep's frontier reached two days past that.
                Assert.assertEquals("the sweep must have written four rows and lost one", 3, lvRowCount(lvToken));
                Assert.assertEquals(4, instance.getLvRowsTotal());

                execute("ALTER LIVE VIEW lv SET TTL 0 HOURS");
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertTtl("lv", 0);

                // 1970-01-02 and 1970-01-03 would have gone with the sweep's next two commits under
                // the TTL. The rn column is what pins the other half: the window ran over all seven
                // days, so the survivors carry their own ordinals and no row was written twice.
                assertQuery("SELECT ts, x, rn FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-02T00:00:00.000000Z\t2\t2
                                1970-01-03T00:00:00.000000Z\t3\t3
                                1970-01-04T00:00:00.000000Z\t4\t4
                                1970-01-05T00:00:00.000000Z\t5\t5
                                1970-01-06T00:00:00.000000Z\t6\t6
                                1970-01-07T00:00:00.000000Z\t7\t7
                                """);
                Assert.assertEquals("the evicted row must stay off the lifetime counter", 6, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertLadder(instance, ts("1970-01-07"), 6);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            restartAndAssertRestoredFromTimeline();
            assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n2\n3\n4\n5\n6\n7\n");
            Assert.assertEquals(6, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRestartAfterClearingTtlMidSeedStillReplacesUnprovenOutput() throws Exception {
        // The cleared-TTL shape of the seed resume. A sweep interrupted after one of its commits
        // evicted a day may not resume off its partial output: the newest seed root stands at an
        // emitted position the table no longer holds. Clearing the TTL before the interruption
        // changes nothing about that, and the retention marker is what says so - a resume that
        // asked whether the view HAS a TTL rather than whether a removal HAPPENED would read a
        // cleared TTL as an untouched prefix, skip-write the rows already on disk and append the
        // tail a second time.
        //
        // The re-sweep then runs with no TTL, so it re-derives all seven days off the base: the
        // rows the eviction took come back, which is section 2.2's rule that recovery can undo a
        // removal, and is the outcome a resume could not produce.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        assertMemoryLeak(() -> {
            createSevenDaySeedBase();
            createSeedView("TTL 2 DAYS ");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedTurnsUntilEviction(job);
                execute("ALTER LIVE VIEW lv SET TTL 0 HOURS");
                final LiveViewInstance instance = driveSeedTurnsUntil(
                        job,
                        () -> {
                            try (TableMetadata metadata = engine.getTableMetadata(lvToken)) {
                                return metadata.getTtlHoursOrMonths() == 0;
                            }
                        },
                        "the seeding view never applied the cleared TTL"
                );
                Assert.assertTrue(
                        "the sweep must still be mid-flight when the process ends",
                        instance.getLvRowsTotal() < 7
                );
                // The evidence the resume has to key on outlives the TTL that caused it.
                assertRetentionMarker(lvToken, true);
            }

            final LogCapture capture = new LogCapture();
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                capture.waitFor("live view seed sweep replacing unproven durable output [view=lv");
            } finally {
                capture.stop();
            }

            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-02T00:00:00.000000Z\t2\t2
                            1970-01-03T00:00:00.000000Z\t3\t3
                            1970-01-04T00:00:00.000000Z\t4\t4
                            1970-01-05T00:00:00.000000Z\t5\t5
                            1970-01-06T00:00:00.000000Z\t6\t6
                            1970-01-07T00:00:00.000000Z\t7\t7
                            """);
            Assert.assertEquals(7, reloaded.getLvRowsTotal());
            Assert.assertFalse(reloaded.hasPendingPartitionRemovals());
            assertRetentionMarker(lvToken, false);
            assertLadder(reloaded, ts("1970-01-07"), 7);
            assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                    .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testWallClockTtlKeepsWhatAFutureDatedFrontierWouldEvict() throws Exception {
        // TTL measures partition age against the SMALLER of the table frontier and the wall clock,
        // and the wall clock is the half that bounds a base row dated in the future. A live view
        // that takes one carries it into its own table, so a view whose base stalls at yesterday and
        // then receives a single row dated next week would lose everything below that row's TTL
        // window if the frontier alone drove eviction. Nothing is evicted while the clock stands
        // where it did; the days go later, when the clock itself reaches them.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                flushRow(job, "1970-01-02", 2, 2);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                execute("ALTER LIVE VIEW lv SET TTL 1 DAY");
                driveLiveViewWalApply(job);

                // The clock is still parked under the data, so this row's own partition is eight
                // days past the frontier and both older days are further past it than the TTL.
                execute("INSERT INTO base VALUES ('1970-01-10T00:00:00.000000Z', 10)");
                driveUntilDurableRowCount(job, 3);
                driveRefreshToQuiescence(job);
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-01
                                1970-01-02
                                1970-01-10
                                """);
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n1\n2\n10\n");
                Assert.assertEquals(3, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());

                // Move the clock past both older days' TTL window. The eviction the future-dated row
                // did not cause happens now, and it stops where the clock stops: the row dated
                // 1970-01-10 is not old against a clock at 1970-01-05, whatever the frontier says.
                setCurrentMicros(ts("1970-01-05T00:00:00.000000Z"));
                execute("INSERT INTO base VALUES ('1970-01-11T00:00:00.000000Z', 11)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(engine.verifyTableName("lv"))) {
                                return reader.size() == 2 && reader.getMinTimestamp() == ts("1970-01-10");
                            }
                        },
                        "the flush never evicted the two days the clock left behind"
                );
                driveRefreshToQuiescence(job);
                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-10
                                1970-01-11
                                """);
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n10\n11\n");
                Assert.assertEquals(2, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTtlWithoutTheWallClockEvictsAgainstAFutureDatedFrontier() throws Exception {
        // The counterexample the case above rests on. With cairo.ttl.use.wall.clock off, TTL judges
        // age by the table frontier alone, so the same future-dated row takes both older days with
        // it on the commit that lands it - and the view's counter, ladder and tier follow the
        // removal the same way they follow any other eviction. This is the opt-out documented for
        // tables; a live view inherits it, since its durable tier is one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_TTL_USE_WALL_CLOCK, "false");
        assertMemoryLeak(() -> {
            createBaseAndView("PARTITION BY DAY");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushRow(job, "1970-01-01", 1, 1);
                flushRow(job, "1970-01-02", 2, 2);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                execute("ALTER LIVE VIEW lv SET TTL 1 DAY");
                driveLiveViewWalApply(job);

                execute("INSERT INTO base VALUES ('1970-01-10T00:00:00.000000Z', 10)");
                driveUntil(
                        job,
                        () -> {
                            try (TableReader reader = engine.getReader(lvToken)) {
                                return reader.size() == 1 && reader.getMinTimestamp() == ts("1970-01-10");
                            }
                        },
                        "the flush never evicted the two days below the future-dated row"
                );
                driveRefreshToQuiescence(job);

                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-10
                                """);
                assertQuery("SELECT x FROM lv").noLeakCheck().expectSize().returns("x\n10\n");
                Assert.assertEquals("both evicted rows must leave the lifetime counter", 1, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertRoutingMode(LiveViewRecordCursor.ROUTING_SEAM);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTtlEvictionDuringSeedCompletesWithoutDuplicating() throws Exception {
        // TTL enforcement stays live while the view is SEEDING, so the sweep's own commits evict
        // days its earlier turns wrote. The sweep has to ride that out, because its coordinates
        // are the base cursor offset and the emitted-output total and neither may follow the
        // table's shrinking row count. Two things then have to hold at the end: the window values
        // must be the ones a single uninterrupted pass over the whole base produces - which is
        // what rn pins, the survivors carrying 3, 4 and 5 rather than 1, 2 and 3 - and the rows
        // that went have to leave lvRowsTotal before the completion boundary seals the head root
        // that carries it, or the seal would record a mismatch and retire the fresh history.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        assertMemoryLeak(() -> {
            createSeedBase();
            createSeedView("TTL 2 DAYS ");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertSurvivingSeedRows();
                Assert.assertEquals("the evicted rows must leave the lifetime counter", 3, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                // The completion boundary retires the sweep's own roots and seals one over the
                // finished state, at the position the table can account for.
                assertTimelineExists(lvToken, true);
                assertLadder(instance, ts("1970-01-05"), 3);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }

            // The head the seed sealed is restorable, so the restart takes the timeline rather
            // than re-deriving, and the evicted days stay evicted.
            restartAndAssertRestoredFromTimeline();
            assertSurvivingSeedRows();
            Assert.assertEquals(3, engine.getLiveViewRegistry().getViewInstance("lv").getLvRowsTotal());
        });
    }

    @Test
    public void testRestartAfterTtlEvictionMidSeedReplacesUnprovenOutput() throws Exception {
        // The same sweep, interrupted after one of its commits evicted a day. The durable output
        // is no longer the prefix the resumed sweep would recompute: the newest seed root stands
        // at an emitted position the table no longer holds, and the table's own row count - which
        // is what the skip-write floor reads - now names a smaller ordinal than the rows already
        // written. Resuming off either would re-emit rows on top of the retained ones.
        //
        // The retention marker is what makes that detectable, so the resume abandons the partial
        // output and re-sweeps from the membership lower bound behind a full-range replacement.
        // The result must be exactly what an uninterrupted seed produces, which is what the shared
        // assertion pins: without the reset the re-sweep skip-writes the surviving rows and
        // appends the tail a second time.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        assertMemoryLeak(() -> {
            createSeedBase();
            createSeedView("TTL 2 DAYS ");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = driveSeedTurnsUntilEviction(job);
                Assert.assertTrue(
                        "the sweep must still be mid-flight when the process ends",
                        instance.getLvRowsTotal() < 5
                );
                assertRetentionMarker(lvToken, true);
            }

            final LogCapture capture = new LogCapture();
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                capture.waitFor("live view seed sweep replacing unproven durable output [view=lv");
            } finally {
                capture.stop();
            }

            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            assertSurvivingSeedRows();
            Assert.assertEquals(3, reloaded.getLvRowsTotal());
            Assert.assertFalse(reloaded.hasPendingPartitionRemovals());
            assertRetentionMarker(lvToken, false);
            assertLadder(reloaded, ts("1970-01-05"), 3);
            assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                    .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testSeedResetWhoseReplacementFailsToSequenceResetsAgain() throws Exception {
        // The first step of the reset's crash matrix: the replacement that discards the unproven
        // output never reaches the sequencer. The view's _txnlog takes one part file per transaction
        // here, so refusing to open the part the replacement's record would land in fails exactly
        // that commit, before any seqTxn is minted - the state a crash inside the commit leaves too.
        //
        // Nothing durable moved: no replacement is sequenced, the table still holds the output the
        // reset could not prove, and the retention marker that says so still stands, because the
        // retire waits for the commit. The turn had fed 1970-01-01 into the accumulators, so the
        // refresh failure handler re-arms the resume in this same process, and the re-armed setup
        // has to reach the verdict the first one reached - reset again - off that same marker. What
        // it must not do is carry on from the failed turn, whose accumulators counted a row that no
        // commit made durable.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1); // one _txnlog part per transaction
        final String[] lvDir = new String[1];
        final String[] failedPartSuffix = new String[1];
        final AtomicBoolean partOpenFailed = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final String partSuffix = failedPartSuffix[0];
                if (partSuffix != null
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.endsWithAscii(name, partSuffix)) {
                    failedPartSuffix[0] = null;
                    partOpenFailed.set(true);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }, () -> {
            interruptSeedAfterItsFirstEviction();
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();
            final long lastTxn = engine.getTableSequencerAPI().lastTxn(lvToken);

            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                capture.start();
                // Transaction lastTxn + 1 is recorded in part lastTxn.
                failedPartSuffix[0] = WalUtils.TXNLOG_PARTS_DIR + Files.SEPARATOR + lastTxn;
                job.run();
                drainWalQueue();
                Assert.assertTrue("the replacement's sequencing must actually have been failed", partOpenFailed.get());
                capture.drain();
                capture.assertLogged("could not apply transaction to WAL table sequencer [table=" + lvToken.getDirName());
                capture.assertLogged("live view mid-seed refresh failure, sweep will resume [view=lv");
                capture.stop();

                Assert.assertEquals("nothing may be sequenced", lastTxn, engine.getTableSequencerAPI().lastTxn(lvToken));
                Assert.assertEquals("the table still holds the unproven output", 3, lvRowCount(lvToken));
                assertRetentionMarker(lvToken, true);
                Assert.assertFalse("the failure must re-arm the resume setup", instance.isSeedResumeAttempted());
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals("a mid-seed fault must not charge the retry budget", 0, instance.getFlushRetryCount());
                Assert.assertFalse(instance.isInvalid());

                // The re-armed setup resets again, off the marker the failed commit left standing.
                capture.start();
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertSeedFinishedAfterItsFirstEviction(instance);
                Assert.assertEquals("exactly the injected fault, and no other", 1, instance.getRefreshFaultCount());
                capture.drain();
                capture.assertLogged("live view seed sweep replacing unproven durable output [view=lv, onDiskLvRows=3, retentionMarkerSeqTxn=");
                capture.assertNotLogged("retentionMarkerSeqTxn=null");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            assertSurvivingSeedRows();
        });
    }

    @Test
    public void testSeedResetWhoseReplacementDidNotApplyHoldsTheSweepUntilItLands() throws Exception {
        // The second step: the replacement is sequenced and its apply fails. The fault is a real
        // one - the replacement's new 1970-01-01 partition cannot open its column file - so the apply
        // job suspends the view's table, and the table keeps the output the reset exists to discard
        // while the WAL carries what replaces it. The retire has already run, since it follows the
        // commit, so the retention marker is gone and the unapplied replacement is now the only
        // evidence that the table is not to be trusted.
        //
        // Section 3.7 rule 3 says a committed-but-unapplied reset may not be followed by new appends,
        // and the sweep followed it anyway: the reset turn sealed a seed root at a position the table
        // did not hold, and the next turn appended 1970-01-02 behind the replacement. With the fault
        // still standing the sweep ran on to its completion, whose head seal declined over the
        // outstanding apply, and left the view ACTIVE with no timeline. The sweep now hands the
        // replacement to the reconciliation gate an out-of-order repair's unapplied replacement
        // already blocks on: while the fault stands, every turn re-drives the apply and nothing else
        // runs; once it clears, the gate lands the replacement and the sweep carries on from the
        // offset the reset turn reached, without resetting a second time.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        final String[] lvDir = new String[1];
        final AtomicBoolean failReplacementPartition = new AtomicBoolean();
        final AtomicInteger replacementPartitionFaults = new AtomicInteger();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failReplacementPartition.get()
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.containsAscii(name, "1970-01-01.")
                        && Utf8s.endsWithAscii(name, "x.d")) {
                    replacementPartitionFaults.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }, () -> {
            interruptSeedAfterItsFirstEviction();
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();
            final long lastTxn = engine.getTableSequencerAPI().lastTxn(lvToken);
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);

            final LogCapture capture = new LogCapture();
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                failReplacementPartition.set(true);
                job.run();
                drainWalQueue();
                Assert.assertTrue("the replacement's apply must actually have been failed", replacementPartitionFaults.get() > 0);
                Assert.assertEquals("the replacement must be sequenced", lastTxn + 1, engine.getTableSequencerAPI().lastTxn(lvToken));
                Assert.assertEquals("and must not be applied", lastTxn, tracker.getWriterTxn());
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(lvToken));
                assertQuery("SELECT view_status FROM live_views()").noLeakCheck().noRandomAccess().returns("view_status\nsuspended\n");
                Assert.assertEquals("the table still holds the output the reset discards", 3, lvRowCount(lvToken));
                assertRetentionMarker(lvToken, false);
                // No seed root over a replacement the table does not hold.
                assertTimelineExists(lvToken, false);
                Assert.assertEquals(lastTxn + 1, instance.getPendingReplacementLvSeqTxn());
                Assert.assertEquals(1, instance.getSeedDataOffset());
                Assert.assertEquals(1, instance.getLvRowsTotal());
                capture.drain();
                capture.assertLogged("live view seed reset replacement committed but did not apply, deferring the sweep [view=lv");

                // While the fault stands, each turn re-drives the apply and appends nothing.
                for (int i = 0; i < 3; i++) {
                    job.run();
                    drainWalQueue();
                }
                Assert.assertEquals("no append may follow the unapplied reset", lastTxn + 1, engine.getTableSequencerAPI().lastTxn(lvToken));
                Assert.assertEquals(lastTxn, tracker.getWriterTxn());
                Assert.assertEquals(1, instance.getSeedDataOffset());
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
                assertTimelineExists(lvToken, false);
                Assert.assertEquals(lastTxn + 1, instance.getPendingReplacementLvSeqTxn());

                failReplacementPartition.set(false);
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                capture.waitFor("live view deferred replacement applied, resuming refresh [view=lv, lvSeqTxn=" + (lastTxn + 1));
                capture.drain();
                capture.assertOnlyOnce("live view seed sweep replacing unproven durable output \\[view=lv");
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));

                assertSeedFinishedAfterItsFirstEviction(instance);
                // The apply failures are the apply job's, which suspends the table and returns;
                // none of them is a refresh fault.
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            assertSurvivingSeedRows();
        });
    }

    @Test
    public void testRestartOverASeedResetWhoseReplacementDidNotApplyResetsAgain() throws Exception {
        // The same failed apply, followed by the end of the process instead of a recovery in it. The
        // restart finds the table still holding the output the reset discards, no retention marker
        // and no timeline - the retire ran with the commit, and the reset turn sealed nothing over
        // its unapplied replacement - with that replacement committed in the view's WAL.
        //
        // The resume setup applies the view's WAL before it reads anything off the table, so the
        // replacement lands first and the table holds the one row it carried. Nothing proves that row
        // either, so the sweep resets again - a table with rows and no root always does - and the
        // second replacement lays the re-swept prefix down over it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        final String[] lvDir = new String[1];
        final AtomicBoolean failReplacementPartition = new AtomicBoolean();
        final AtomicInteger replacementPartitionFaults = new AtomicInteger();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failReplacementPartition.get()
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.containsAscii(name, "1970-01-01.")
                        && Utf8s.endsWithAscii(name, "x.d")) {
                    replacementPartitionFaults.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }, () -> {
            interruptSeedAfterItsFirstEviction();
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();
            final long lastTxn = engine.getTableSequencerAPI().lastTxn(lvToken);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                failReplacementPartition.set(true);
                job.run();
                drainWalQueue();
                failReplacementPartition.set(false);
                Assert.assertTrue("the replacement's apply must actually have been failed", replacementPartitionFaults.get() > 0);
                Assert.assertEquals(lastTxn + 1, engine.getTableSequencerAPI().lastTxn(lvToken));
                Assert.assertEquals(lastTxn, engine.getTableSequencerAPI().getTxnTracker(lvToken).getWriterTxn());
                Assert.assertEquals(3, lvRowCount(lvToken));
                assertRetentionMarker(lvToken, false);
                assertTimelineExists(lvToken, false);
            }

            final LogCapture capture = new LogCapture();
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                capture.waitFor("live view seed sweep replacing unproven durable output [view=lv, onDiskLvRows=1, retentionMarkerSeqTxn=null]");
            } finally {
                capture.stop();
            }

            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            assertSeedFinishedAfterItsFirstEviction(reloaded);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));
            assertNoRefreshFaults("lv");

            restartAndAssertRestoredFromTimeline();
            assertSurvivingSeedRows();
        });
    }

    @Test
    public void testRestartOverAnUnappliedSeedAppendWaitsForItBehindAHalfInitialisedTracker() throws Exception {
        // The third step: the reset has landed and the append after it commits, but its apply fails,
        // and the process ends there. The reset sealed its one root at 1970-01-01 - one row per turn
        // with no row or duration cadence to seal on - so the root now sits behind a table that
        // holds 1970-01-01 and 1970-01-02, while the view's WAL also carries 1970-01-03. No removal
        // happened after the reset, so the restart's resume is the proven kind, and its skip-write
        // floor is the table's row count - which names the emitted ordinal only once the append has
        // landed. The resume setup therefore applies the view's WAL first and refuses to read the
        // table until the apply has caught up.
        //
        // It judged "caught up" off the SeqTxnTracker alone. A restart hands the sweep a tracker
        // holding neither number until CheckWalTransactionsJob gets to it, and the setup's own apply
        // then stamps the writer txn - its failure path records where the writer stopped - while the
        // committed seqTxn stays unset. A real writer txn against an unset seqTxn read as caught up,
        // so with the apply failing again on the first turn after the restart the setup read two
        // rows, took a floor of 2 off root@1, skip-wrote 1970-01-02 and appended 1970-01-03 behind
        // the block that already carried it: two rows at 1970-01-03, both numbered 3, once both
        // applied. The check now reads disk truth for a tracker missing either number, so that turn
        // waits, and the next one finds the append applied and floors at 3.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_TURN_MAX_DURATION_MICROS, 0); // one row per seed turn
        final String[] lvDir = new String[1];
        final AtomicBoolean failAppendPartition = new AtomicBoolean();
        final AtomicInteger appendPartitionFaults = new AtomicInteger();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failAppendPartition.get()
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.containsAscii(name, "1970-01-03.")
                        && Utf8s.endsWithAscii(name, "x.d")) {
                    appendPartitionFaults.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }, () -> {
            interruptSeedAfterItsFirstEviction();
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The reset turn and the append after it, both applied.
                final LiveViewInstance instance = driveSeedTurnsUntil(
                        job,
                        () -> {
                            final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                            return lv.isSeedResumeAttempted() && lv.getLvRowsTotal() == 2;
                        },
                        "the re-sweep never reached its second row"
                );
                Assert.assertEquals(2, lvRowCount(lvToken));
                assertRetentionMarker(lvToken, false);
                assertLadder(instance, ts("1970-01-01"), 1);

                failAppendPartition.set(true);
                job.run();
                drainWalQueue();
                Assert.assertTrue("the append's apply must actually have been failed", appendPartitionFaults.get() > 0);
                Assert.assertEquals(3, instance.getLvRowsTotal());
                Assert.assertEquals("the append must be sequenced and not applied", 2, lvRowCount(lvToken));
                assertLadder(instance, ts("1970-01-01"), 1);
            }

            final LogCapture capture = new LogCapture();
            // A restart down to the sequencer's trackers, which come back holding neither number.
            engine.getLiveViewRegistry().clear();
            engine.getTableSequencerAPI().releaseAll();
            engine.buildViewGraphs();
            Assert.assertFalse(engine.getTableSequencerAPI().isTxnTrackerInitialised(lvToken));
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                // No WAL drain ahead of this turn: CheckWalTransactionsJob would initialise the tracker.
                job.run();
                Assert.assertTrue("the first apply after the restart must fail too", appendPartitionFaults.get() > 1);
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(lvToken);
                Assert.assertTrue("the failed apply stamps the writer txn", tracker.isInitialised());
                Assert.assertEquals(
                        "and nothing has stamped the committed seqTxn yet",
                        SeqTxnTracker.UNINITIALIZED_TXN,
                        tracker.getSeqTxn()
                );
                Assert.assertFalse(
                        "the resume must wait for the append rather than read a table missing it",
                        reloaded.isSeedResumeAttempted()
                );
                Assert.assertEquals(2, lvRowCount(lvToken));

                failAppendPartition.set(false);
                driveSeedTurnsUntil(job, reloaded::isSeedResumeAttempted, "the resume never ran once the append could land");
                Assert.assertEquals(
                        "the floor must count the append the restart found unapplied",
                        3,
                        reloaded.getSeedSkipWriteFloor()
                );
                Assert.assertEquals(3, lvRowCount(lvToken));

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                capture.drain();
                capture.assertNotLogged("live view seed sweep replacing unproven durable output [view=lv");

                assertSeedFinishedAfterItsFirstEviction(reloaded);
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }

            restartAndAssertRestoredFromTimeline();
            assertSurvivingSeedRows();
        });
    }

    @Test
    public void testSeedCompletionWhosePromotionDidNotPersistReSeedsOnRestart() throws Exception {
        // The last step: the sweep completes but its promotion to ACTIVE never reaches _lv.s. The
        // completion retires the sweep's roots, settles the counter, seals the finished head and
        // only then persists the state that says ACTIVE - so a failure there, or a crash at that
        // point, leaves the finished output and a steady head root on disk under a state file that
        // still says SEEDING. The in-memory view has flipped; the durable one has not.
        //
        // The restart reads SEEDING and runs the sweep's resume setup, where the head is no seed
        // resume point: it carries no cursor offset, so nothing proves which prefix of a sweep the
        // three rows on disk are, and the sweep resets over them. The retention marker went with the
        // completion's retire, so the reset keys on the missing resume point alone. That costs a
        // re-sweep of the whole history, and the result is still exactly the uninterrupted one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        final String[] lvDir = new String[1];
        final AtomicBoolean failStatePersist = new AtomicBoolean();
        final AtomicInteger statePersistFaults = new AtomicInteger();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failStatePersist.get()
                        && Utf8s.containsAscii(name, lvDir[0])
                        && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    statePersistFaults.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }, () -> {
            interruptSeedAfterItsFirstEviction();
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();

            final LogCapture capture = new LogCapture();
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Every row re-swept; the next turn is the completion.
                final LiveViewInstance instance = driveSeedTurnsUntil(
                        job,
                        () -> {
                            final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                            return lv.isSeedResumeAttempted() && lv.getSeedDataOffset() == 5;
                        },
                        "the re-sweep never fed its last row"
                );
                failStatePersist.set(true);
                job.run();
                failStatePersist.set(false);
                drainWalQueue();
                // Both writes the completion attempts: the consumed-seqTxn advance, and the plain
                // state persist it falls back on.
                Assert.assertEquals(2, statePersistFaults.get());
                capture.drain();
                capture.assertLogged("could not advance live view consumed seqTxn after seed sweep [view=lv");
                Assert.assertEquals(
                        "the in-memory view flips before the persist",
                        LiveViewState.SEED_STATE_ACTIVE,
                        instance.getStateReader().getSeedState()
                );
                assertLadder(instance, ts("1970-01-05"), 3);
                assertRetentionMarker(lvToken, false);
                Assert.assertEquals(3, instance.getLvRowsTotal());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
            } finally {
                capture.stop();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertEquals(
                    "the durable state never left SEEDING",
                    LiveViewState.SEED_STATE_SEEDING,
                    reloaded.getStateReader().getSeedState()
            );
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                capture.waitFor("live view timeline holds no seed resume point [view=lv");
                capture.waitFor("live view seed sweep replacing unproven durable output [view=lv, onDiskLvRows=3, retentionMarkerSeqTxn=null]");
            } finally {
                capture.stop();
            }

            assertSeedFinishedAfterItsFirstEviction(reloaded);
            assertNoRefreshFaults("lv");

            restartAndAssertRestoredFromTimeline();
            assertSurvivingSeedRows();
        });
    }

    @Test
    public void testTtlEvictionBelowTheResumeFloorStillWritesEveryLaterOutput() throws Exception {
        // The skip-write floor is a durable output POSITION, established once per process by a
        // proven resume, and the sweep keeps writing every output whose emitted ordinal reaches it.
        // A TTL eviction inside the sweep's own commit takes the table below that position - here
        // three of the four rows the floor counts go at once - and the sweep then rides out the
        // rest of its turns with a floor that names more rows than the table holds.
        //
        // That only works because nothing lowers lvRowsTotal while the view is SEEDING: the sweep
        // settles the removal against the counter once, at the completion boundary, where the
        // counter has no second role left. Settle it in the turn that takes the removal instead -
        // reconcileSeedPartitionRemovals called right after the turn's own apply, the mutation this
        // case was run against - and the next turn re-enters with an emitted ordinal below the floor
        // and skips writing the outputs it recomputes. That is a permanent hole rather than a
        // duplicate: the 300-emitted-rows-lose-their-first-100 case, run forward through a sweep
        // instead of across a restart.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 2); // two rows per seed turn
        assertMemoryLeak(() -> {
            createResumeFloorSeedBase();
            createSeedView("TTL 3 DAYS ");
            final TableToken lvToken = engine.verifyTableName("lv");

            // Two clean turns, four durable rows, nothing evicted - so the restart below finds a
            // root it can prove and a table whose size still names the emitted ordinal.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedTurnsUntil(
                        job,
                        () -> lvRowCount(lvToken) >= 4,
                        "the seed never reached four durable rows"
                );
                assertRetentionMarker(lvToken, false);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // 1970-01-07 carries the view's frontier three days past 1970-01-04's ceiling, so
                // the turn that commits it evicts the first three days. Drive on the table's own
                // size rather than on the pending events: the events are what a mid-sweep
                // reconciliation would consume, and the premise here is the row count.
                final LiveViewInstance instance = driveSeedTurnsUntil(
                        job,
                        () -> lvRowCount(lvToken) < 4,
                        "the seed never evicted below the resume floor"
                );
                Assert.assertEquals(
                        "the resume must be the proven kind, whose floor is the durable row count",
                        4,
                        instance.getSeedSkipWriteFloor()
                );
                Assert.assertFalse(instance.isSeedReplacePending());
                Assert.assertEquals("the table must now hold fewer rows than the floor names", 3, lvRowCount(lvToken));
                Assert.assertEquals("the emitted ordinal must not follow the table down", 6, instance.getLvRowsTotal());
                Assert.assertTrue(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, true);

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // Both outputs the later turns processed are on disk, at the row numbers a single
                // uninterrupted pass produces.
                assertResumeFloorSeedRows();
                Assert.assertEquals("the evicted rows must leave the lifetime counter", 5, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertLadder(instance, ts("1970-01-07T02:00:00.000000Z"), 5);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testInProcessSeedFailureAfterAnEvictionDoesNotReuseTheResumeFloor() throws Exception {
        // The same eviction, followed by a turn that faults before it commits. A mid-seed fault
        // re-arms the resume in the SAME process - rebuildWindowStateAfterMidDrainFailure keeps the
        // pinned base snapshot and clears the resume-attempted flag - so the next turn runs the
        // whole resume setup again over a table a removal has already holed.
        //
        // It must reach the same verdict a restart does: the retention marker stands (a SEEDING view
        // defers its reconciliation), so no root is trusted, the sweep re-runs from the membership
        // lower bound behind a full-range replacement, and the floor the earlier resume derived goes
        // back to zero. Carrying that floor into the re-sweep is what "reuse of the old skip-write
        // path" means: the replacement wipes the durable range and the first four recomputed outputs
        // are then skip-written into it, so 1970-01-04 - the one row below the floor that survives to
        // the end - never comes back. Re-deriving the floor from the table's own size instead, which
        // is the literal shape of the old path, holes the re-sweep just as surely but not visibly:
        // the three days it drops are the three the TTL evicts anyway. The floor assertion below is
        // what names that one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 2); // two rows per seed turn
        final String[] baseDir = new String[1];
        final AtomicBoolean armBaseReadFault = new AtomicBoolean();
        final AtomicBoolean baseReadFailed = new AtomicBoolean();
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // The base is hour-partitioned, so the sweep crosses a base partition boundary
                // inside the view's last day - which is what lets the fault land on a turn that has
                // already fed a row (windowStateDirty, the mid-drain rebuild's own guard) rather
                // than on its very first hasNext(). One shot: the re-swept read succeeds.
                if (armBaseReadFault.get()
                        && baseDir[0] != null
                        && Utf8s.endsWithAscii(name, "x.d")
                        && Utf8s.containsAscii(name, baseDir[0])
                        && Utf8s.containsAscii(name, "1970-01-07T02")
                        && !Utf8s.containsAscii(name, "wal")) {
                    armBaseReadFault.set(false);
                    baseReadFailed.set(true);
                    return -1;
                }
                return super.openRO(name);
            }
        }, () -> {
            createResumeFloorSeedBase();
            createSeedView("TTL 3 DAYS ");
            final TableToken lvToken = engine.verifyTableName("lv");
            baseDir[0] = engine.verifyTableName("base").getDirName();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedTurnsUntil(
                        job,
                        () -> lvRowCount(lvToken) >= 4,
                        "the seed never reached four durable rows"
                );
                assertRetentionMarker(lvToken, false);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = driveSeedTurnsUntil(
                        job,
                        () -> lvRowCount(lvToken) < 4,
                        "the seed never evicted below the resume floor"
                );
                Assert.assertEquals(4, instance.getSeedSkipWriteFloor());
                Assert.assertEquals(3, lvRowCount(lvToken));
                Assert.assertEquals(6, instance.getLvRowsTotal());
                Assert.assertTrue(instance.hasPendingPartitionRemovals());

                armBaseReadFault.set(true);
                job.run();
                drainWalQueue();
                Assert.assertTrue("the mid-seed read must actually have been failed", baseReadFailed.get());
                Assert.assertEquals("the faulting turn commits nothing", 3, lvRowCount(lvToken));
                Assert.assertEquals(
                        "the faulting turn leaves the emitted ordinal where the last commit put it",
                        6,
                        instance.getLvRowsTotal()
                );
                Assert.assertEquals("a mid-seed fault must not charge the retry budget", 0, instance.getFlushRetryCount());
                Assert.assertFalse("a mid-seed fault must not invalidate the view", instance.isInvalid());

                // The re-armed turn runs the resume setup again, in this same process.
                driveSeedTurnsUntil(
                        job,
                        () -> instance.getSeedSkipWriteFloor() == 0,
                        "the re-armed resume setup never cleared the floor the earlier resume derived"
                );
                capture.waitFor("live view seed sweep replacing unproven durable output [view=lv");

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // The re-sweep re-derives every output off the base, so the result is the one an
                // uninterrupted seed produces - 1970-01-04 included.
                assertResumeFloorSeedRows();
                Assert.assertEquals(5, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertLadder(instance, ts("1970-01-07T02:00:00.000000Z"), 5);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                Assert.assertEquals("exactly the injected fault, and no other", 1, instance.getRefreshFaultCount());
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testDropPartitionWhileSeedingReconcilesTheCounterAtCompletion() throws Exception {
        // A DROP PARTITION sequenced against a SEEDING view is applied by the next sweep turn,
        // whose own apply carries it. The sweep keeps its cursor and its emitted-output total,
        // so the rows the DROP took do NOT come back - it removed output the sweep had already
        // written and moved past - and the completion boundary takes them off the counter.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        assertMemoryLeak(() -> {
            createSeedBase();
            createSeedView("");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = driveSeedTurnsUntil(
                        job,
                        () -> lvRowCount(lvToken) >= 3,
                        "the seed never reached three durable rows"
                );
                // 1970-01-01 is three partitions below the one the sweep is writing, so the
                // active-partition guard admits it.
                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '1970-01-01'");
                driveSeedTurnsUntil(
                        job,
                        instance::hasPendingPartitionRemovals,
                        "the seed never applied the DROP PARTITION"
                );

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                assertQuery("SELECT ts, x, rn FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\trn
                                1970-01-02T00:00:00.000000Z\t2\t2
                                1970-01-03T00:00:00.000000Z\t3\t3
                                1970-01-04T00:00:00.000000Z\t4\t4
                                1970-01-05T00:00:00.000000Z\t5\t5
                                """);
                Assert.assertEquals("the dropped row must leave the lifetime counter", 4, instance.getLvRowsTotal());
                Assert.assertFalse(instance.hasPendingPartitionRemovals());
                assertRetentionMarker(lvToken, false);
                assertLadder(instance, ts("1970-01-05"), 4);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testSeedResetWithNoQualifyingRowClearsThePartialOutput() throws Exception {
        // The reset's other half: a re-seed that qualifies no row at all still owes the deletion.
        // The base loses every row while the view is mid-sweep and its timeline is gone, so the
        // resumed sweep finds nothing to emit - and the partial output it wrote earlier must go
        // with the same replacement a non-empty re-seed would have carried, rather than survive
        // because there was no row to trigger a commit.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per seed turn
        assertMemoryLeak(() -> {
            createSeedBase();
            createSeedView("");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = driveSeedTurnsUntil(
                        job,
                        () -> lvRowCount(lvToken) >= 2,
                        "the seed never reached two durable rows"
                );
                // No root survives to prove what the two durable rows are.
                retireSeedCheckpointTimeline(instance);
            }
            execute("ALTER TABLE base DROP PARTITION WHERE ts < '1970-01-06'");
            drainWalQueue();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            Assert.assertEquals(0, reloaded.getLvRowsTotal());
            Assert.assertEquals(
                    "the seed must still complete",
                    LiveViewState.SEED_STATE_ACTIVE,
                    reloaded.getStateReader().getSeedState()
            );
            assertRetentionMarker(lvToken, false);
            assertNoRefreshFaults("lv");
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
    public void testOutOfOrderRepairOverParquetPartitionCompletes() throws Exception {
        // A live view repairs an out-of-order base commit by publishing a REPLACE_RANGE over its
        // own table, and the range the head-miss replay publishes runs from the view's lower
        // bound, so the out-of-order row does not have to land inside a Parquet partition for the
        // replacement to cover it: the row this test inserts sits an hour ABOVE the Parquet
        // partition and the replacement reaches it anyway. Any out-of-order base commit under a
        // view holding any Parquet partition takes this path.
        //
        // TableWriter.processO3Block used to refuse replace mode against a Parquet partition
        // outright ("commit replace mode is not supported for Parquet partitions"), a critical
        // error that suspended the view. The writer now decodes the Parquet partitions the range
        // covers, applies the replacement over native storage and re-encodes them afterwards, so
        // the repair lands and the partition the user compacted is Parquet again.
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
                driveUntilDurableRowCount(job, 4);
                driveRefreshToQuiescence(job);

                Assert.assertFalse(
                        "the replacement must not suspend the view's own table",
                        engine.getTableSequencerAPI().isSuspended(lvToken)
                );
                assertNoRefreshFaults("lv");
                // The repaired view holds the out-of-order row in ts order, with the window
                // function recomputed over it: 'a' now counts three rows, not two.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tx\tsym\tv\trn
                                1970-01-01T01:00:00.000000Z\t1\ta\talpha\t1
                                1970-01-01T02:00:00.000000Z\t3\ta\tgamma\t2
                                1970-01-01T02:30:00.000000Z\t2\ta\tbeta\t3
                                1970-01-01T03:00:00.000000Z\t4\tb\tdelta\t1
                                """);
                // The view matches a recompute of its own SELECT from the base table.
                assertSqlCursors(
                        "SELECT ts, x, sym, v, count(*) OVER (PARTITION BY sym ORDER BY ts " +
                                "ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base",
                        "SELECT ts, x, sym, v, rn FROM lv"
                );
                // The compacted partition survived the repair as Parquet.
                assertParquetPartitionCount(1);
                assertQuery("SELECT name, numRows, isParquet FROM table_partitions('lv')")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                name\tnumRows\tisParquet
                                1970-01-01T01\t1\ttrue
                                1970-01-01T02\t2\tfalse
                                1970-01-01T03\t1\tfalse
                                """);
            }
        });
    }

    @Test
    public void testResumeAnchorReadsItsTimestampGroupThroughAParquetBoundary() throws Exception {
        // Section 3.4's counterexample with the boundary partition in Parquet, which is where
        // the row-position search used to give up: countDurableRowsBelow returned -1 for a
        // non-native boundary partition and coversOwnTimestampGroup read that absence as no
        // evidence either way, so the under-covering root stood. The resume then restored
        // state that had seen one row of a three-row timestamp group and replayed above it,
        // and every value it computed from then on was short by the two rows it never read -
        // durably, and with nothing left to detect it but the row-count drift guard, which
        // retires the ladder and leaves the wrong rows on disk.
        //
        // The search now reads a Parquet boundary through the _pm sidecar - the row groups
        // below it contribute their recorded size, the one the boundary falls inside is
        // decoded and binary-searched - so the root is refused here exactly as its native
        // twin is in testRetentionLowersARootByItsExactDeltaNotAFreshCount.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, count(*) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 3, 3);
                // The tie, as in the native case: a row on the frontier's own timestamp is an
                // ordinary forward append and the seal that follows it has no boundary above
                // the head to open, so the 03:00:10 root keeps covering one row of three.
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 4, 4);
                flushOneRow(job, "2026-01-01T03:00:10.000000Z", 5, 5);
                flushOneRow(job, "2026-01-01T03:01:00.000000Z", 6, 6);
                // One hour above, so the tie's own partition is no longer the active one and
                // can be converted.
                flushOneRow(job, "2026-01-01T04:00:00.000000Z", 7, 7);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                assertLadder(
                        instance,
                        ts("2026-01-01T01:00:10.000000Z"), 1,
                        ts("2026-01-01T02:00:10.000000Z"), 2,
                        ts("2026-01-01T03:00:10.000000Z"), 3,
                        ts("2026-01-01T03:01:00.000000Z"), 6,
                        ts("2026-01-01T04:00:00.000000Z"), 7
                );

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '2026-01-01T03'");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);
                assertParquetPartitionCount(1);

                execute("ALTER LIVE VIEW lv DROP PARTITION LIST '2026-01-01T01'");
                driveLiveViewWalApply(job);
                assertLadder(
                        instance,
                        ts("2026-01-01T02:00:10.000000Z"), 1,
                        ts("2026-01-01T03:00:10.000000Z"), 2,
                        ts("2026-01-01T03:01:00.000000Z"), 5,
                        ts("2026-01-01T04:00:00.000000Z"), 6
                );
                assertRetentionMarker(lvToken, false);
                assertNoRefreshFaults("lv");

                // The out-of-order row, 30 seconds above the tie and inside the Parquet
                // partition. The plan finds the 03:00:10 root below it, the search reads the
                // partition's four rows at or below 03:00:10 against the 2 the root claims,
                // and re-anchors on 02:00:10.
                capture.start();
                try {
                    execute("INSERT INTO base VALUES ('2026-01-01T03:00:40.000000Z', 'a', 100)");
                    driveUntilDurableRowCount(job, 7);
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view resume anchor no longer covers its timestamp group, re-anchoring below it "
                            + "[view=lv, anchorMaxTs=2026-01-01T03:00:10.000000Z");
                    capture.assertLoggedRE(", lvRowPosition=2]");
                } finally {
                    capture.stop();
                }

                // Every re-emitted row counts the whole tie. An anchor left standing at the
                // tie root would have restored state that had seen one row at 03:00:10 and
                // numbered 03:00:40, 03:01:00 and 04:00:00 4, 5 and 6.
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\trn
                                2026-01-01T02:00:10.000000Z\ta\t2
                                2026-01-01T03:00:10.000000Z\ta\t3
                                2026-01-01T03:00:10.000000Z\ta\t4
                                2026-01-01T03:00:10.000000Z\ta\t5
                                2026-01-01T03:00:40.000000Z\ta\t6
                                2026-01-01T03:01:00.000000Z\ta\t7
                                2026-01-01T04:00:00.000000Z\ta\t8
                                """);
                // The replacement decoded the partition, applied over native storage and
                // re-encoded it, so the compacted hour is Parquet again.
                assertParquetPartitionCount(1);
                assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                        .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testLocalizedRepairOverAParquetFloorKeepsItsCheckpointLadder() throws Exception {
        // The other half of the same search. A localized repair measures the durable prefix
        // below its emit floor R and below its convergence bound H before it stages anything -
        // those two counts are what anchor every root the capture re-versions - and a floor
        // inside a Parquet partition used to report no searchable prefix. That threw, the
        // catch freed the capture, and the view came out of an ordinary out-of-order commit
        // with no checkpoint ladder at all. Compacting cold partitions is what a long-lived
        // view does, so it was one retired timeline per out-of-order base commit from the
        // first conversion onwards.
        //
        // The narrow RANGE frame is what puts the repair on this path rather than on the
        // resume: a correction here converges below the frontier, so the plan names a finite
        // H and rebuilds the interval instead of replaying from an anchor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                    "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base)");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushOneRow(job, "2026-01-01T02:00:10.000000Z", 1, 1);
                flushOneRow(job, "2026-01-01T02:01:00.000000Z", 2, 2);
                flushOneRow(job, "2026-01-01T02:02:00.000000Z", 3, 3);
                flushOneRow(job, "2026-01-01T02:03:00.000000Z", 4, 4);
                // An hour above, so the hour the repair lands in is no longer the active
                // partition and can be converted.
                flushOneRow(job, "2026-01-01T03:00:00.000000Z", 5, 5);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '2026-01-01T02'");
                driveLiveViewWalApply(job);
                driveRefreshToQuiescence(job);
                assertParquetPartitionCount(1);
                final long generationBeforeRepair = readGeneration(lvToken);

                // Out of order, inside the converted hour. Both bounds the capture measures -
                // the emit floor 02:02:30 and the convergence bound just above 02:03:00 - fall
                // inside the Parquet partition, so both counts go through its row groups.
                capture.start();
                try {
                    execute("INSERT INTO base VALUES ('2026-01-01T02:02:30.000000Z', 'a', 100)");
                    driveUntilDurableRowCount(job, 6);
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view O3 head-miss replay completed [view=lv");
                    capture.assertLoggedRE("localized=true, scanLowTs=\\d+, coldKeyed=false, emitLowTs="
                            + ts("2026-01-01T02:02:30.000000Z"));
                    capture.assertNotLogged("could not measure live view durable prefix for a checkpoint timeline repair");
                } finally {
                    capture.stop();
                }

                // The ladder survived the repair rather than going with the capture, and it
                // carries the roots above the correction at their re-derived positions: the
                // 02:03:00 root moved up by the row that landed under it, and so did the head.
                assertTimelineExists(lvToken, true);
                Assert.assertTrue(
                        "the repair must publish a new generation rather than retire the timeline",
                        readGeneration(lvToken) > generationBeforeRepair
                );
                assertLadder(
                        instance,
                        ts("2026-01-01T02:00:10.000000Z"), 1,
                        ts("2026-01-01T02:01:00.000000Z"), 2,
                        ts("2026-01-01T02:02:00.000000Z"), 3,
                        ts("2026-01-01T02:03:00.000000Z"), 5,
                        ts("2026-01-01T03:00:00.000000Z"), 6
                );
                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\ts
                                2026-01-01T02:00:10.000000Z\ta\t1.0
                                2026-01-01T02:01:00.000000Z\ta\t2.0
                                2026-01-01T02:02:00.000000Z\ta\t3.0
                                2026-01-01T02:02:30.000000Z\ta\t103.0
                                2026-01-01T02:03:00.000000Z\ta\t104.0
                                2026-01-01T03:00:00.000000Z\ta\t5.0
                                """);
                // The replacement decoded the partition, applied over native storage and
                // re-encoded it, so the compacted hour is Parquet again.
                assertParquetPartitionCount(1);
                assertNoRefreshFaults("lv");
            }

            // The ladder is not merely present, it is trusted: a restart restores off it
            // rather than rebuilding from the applied base.
            restartAndAssertRestoredFromTimeline();
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

    /**
     * Asserts both {@code _lv} copies - the view's own table directory and the sequencer directory
     * that replicates - carry {@code expectedTtlHoursOrMonths}. The two are written from the same
     * definition object, so a test that read only one would pass while the copy that actually
     * crosses to a replica stayed stale.
     */
    private static void assertDefinitionTtl(String viewName, int expectedTtlHoursOrMonths) {
        final TableToken token = engine.verifyTableName(viewName);
        try (
                BlockFileReader reader = new BlockFileReader(configuration);
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(token).concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME);
            Assert.assertEquals(
                    "table-directory _lv TTL of '" + viewName + "'",
                    expectedTtlHoursOrMonths,
                    LiveViewDefinition.readFromPath(reader, path, token, null, new GenericRecordMetadata())
                            .getTtlHoursOrMonths()
            );
            path.of(configuration.getDbRoot()).concat(token).concat(WalUtils.SEQ_DIR)
                    .concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME);
            Assert.assertEquals(
                    "sequencer-directory _lv TTL of '" + viewName + "'",
                    expectedTtlHoursOrMonths,
                    LiveViewDefinition.readFromPath(reader, path, token, null, new GenericRecordMetadata())
                            .getTtlHoursOrMonths()
            );
        }
    }

    private static void assertTtl(int expectedTtlHoursOrMonths) {
        assertTtl("lv", expectedTtlHoursOrMonths);
    }

    private static void appendKeyedHour(StringBuilder rows, int day, int hour) {
        for (int account = 1; account <= 4; account++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append(keyedRow(day, hour, account * 10, "acct-" + account));
        }
    }

    /**
     * One {@link #createKeyedView} base row of {@code account} at {@code hour}:{@code minute} on
     * 2026-01-{@code day}, as an INSERT tuple.
     */
    private static String keyedRow(int day, int hour, int minute, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z', '" + account + "', 1.0)";
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

    private void assertRepairMarker(TableToken lvToken, boolean expected) {
        try (Path path = new Path()) {
            Assert.assertEquals(
                    "repair marker presence at " + checkpointsDir(path, lvToken),
                    expected,
                    LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), path)
            );
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
    /**
     * Sequences a {@code DROP PARTITION} naming {@code partitionName} straight into the live view's
     * WAL, drives the apply, and asserts the writer's own active-partition guard is what refused
     * it - a tolerated WAL command failure carrying the guard's message, rather than a parse error,
     * a silent no-op or a suspension.
     */
    private void assertRawDropPartitionTolerated(LiveViewRefreshJob job, TableToken lvToken, String partitionName) {
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            sequenceRawDropPartition(lvToken, partitionName);
            driveLiveViewWalApply(job);
            capture.drain();
            capture.assertLoggedRE("tolerated WAL command failure \\[table=" + lvToken.getDirName()
                    + ", seqTxn=\\d+, command=ALTER TABLE, "
                    + "error=cannot drop the active partition of a live view "
                    + "\\[partition=2026-01-01T03:00:00\\.000000Z]");
        } finally {
            capture.stop();
        }
    }

    /**
     * Asserts the published in-memory slot holds exactly {@code expectedRows} rows, the first of
     * them at {@code expectedFirstTimestamp}. This is how a test tells a band that IS the LV
     * table's trailing rows - the identity the seam cuts on - from one still holding rows a
     * removal took out from under it.
     */
    private void assertBand(LiveViewInstance instance, String expectedFirstTimestamp, long expectedRows) {
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull("the view must hold an in-memory tier", tier);
        final LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
        Assert.assertEquals("published slot row count", expectedRows, slot.rowCount());
        Assert.assertEquals(
                "published slot's first row",
                ts(expectedFirstTimestamp),
                slot.getLong(0, instance.getCompiledPlan().getOutputMetadata().getTimestampIndex())
        );
    }

    private void assertRoutingMode(int expectedRoutingMode) throws SqlException {
        try (
                RecordCursorFactory factory = select("SELECT * FROM lv");
                LiveViewRecordCursor cursor = (LiveViewRecordCursor) unwrapLvFactory(factory).getCursor(sqlExecutionContext)
        ) {
            Assert.assertEquals("live view cursor routing mode", expectedRoutingMode, cursor.routingMode());
        }
    }

    /**
     * Reads {@code cursor} from the top and returns the {@code x} of every row it serves, asserting
     * the cursor's own {@code size()} names that many rows. A snapshot pinned across a removal may
     * hold rows the table no longer does; what it may not do is disagree with itself about how many
     * rows it has, which is what a LIMIT and a full scan would then read differently.
     */
    private LongList drainPinnedCursor(LiveViewRecordCursor cursor) {
        final LongList rows = new LongList();
        cursor.toTop();
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            rows.add(record.getLong(1));
        }
        Assert.assertEquals("the cursor's size() must name the rows its scan serves", rows.size(), cursor.size());
        return rows;
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
     * Writes the durable retention marker into the view's {@code _checkpoints}, as
     * {@link io.questdb.cairo.TableWriter} does before the commit that makes a removal durable.
     * The tests that call this directly are pinning the restart rule the record carries rather
     * than driving a removal that would write it.
     */
    private void writeRetentionMarker(TableToken lvToken, long seqTxn) {
        try (Path dir = new Path()) {
            LiveViewRetentionMarker.write(configuration, checkpointsDir(dir, lvToken), lvToken.getTableId(), seqTxn);
        }
    }

    /**
     * Renames a published retention marker back to the {@code .tmp} sibling it is staged
     * through, which is what a crash inside its own publish leaves on disk.
     */
    private void stageRetentionMarker(TableToken lvToken) {
        try (Path dir = new Path(); Path from = new Path(); Path to = new Path()) {
            checkpointsDir(dir, lvToken);
            LiveViewCheckpointLayout.retentionMarkerPath(from, dir);
            LiveViewCheckpointLayout.retentionMarkerPath(to, dir);
            to.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            Assert.assertEquals(
                    "could not stage the retention marker",
                    Files.FILES_RENAME_OK,
                    configuration.getFilesFacade().rename(from.$(), to.$())
            );
        }
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
     * The same one-commit-per-row drive for the three-column {@code (ts, sym, x)} base the
     * out-of-order repair cases build their cadence history over.
     */
    private void flushOneRow(LiveViewRefreshJob job, String timestamp, long x, long expectedDurableRows) throws Exception {
        execute("INSERT INTO base VALUES ('" + timestamp + "', 'a', " + x + ")");
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

    /**
     * The rows a five-day seed under {@code TTL 2 DAYS} leaves behind. The {@code rn} column is
     * the point: the window ran over all five days, so the survivors carry 3, 4 and 5 - a sweep
     * that restarted its accumulators, or one that re-emitted a row it had already written,
     * cannot produce them.
     */
    private void assertSurvivingSeedRows() throws Exception {
        assertQuery("SELECT ts, x, rn FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tx\trn
                        1970-01-03T00:00:00.000000Z\t3\t3
                        1970-01-04T00:00:00.000000Z\t4\t4
                        1970-01-05T00:00:00.000000Z\t5\t5
                        """);
    }

    /**
     * The result {@link #createResumeFloorSeedBase}'s history seeds down to under {@code TTL 3
     * DAYS}: 1970-01-04 is the newest of the four days the pre-restart turns wrote, and the only
     * one of them the view's final frontier still keeps. Every row carries the {@code rn} a single
     * uninterrupted pass over the whole base produces, so a re-fed row shows up as a repeat and a
     * skipped one as a gap - neither of which a row count alone would name.
     */
    private void assertResumeFloorSeedRows() throws Exception {
        assertQuery("SELECT ts, x, rn FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tx\trn
                        1970-01-04T00:00:00.000000Z\t4\t4
                        1970-01-07T00:00:00.000000Z\t5\t5
                        1970-01-07T01:00:00.000000Z\t6\t6
                        1970-01-07T01:30:00.000000Z\t7\t7
                        1970-01-07T02:00:00.000000Z\t8\t8
                        """);
    }

    /**
     * Eight base rows shaped so that a two-row seed turn can cross into a fresh base partition
     * without carrying the view's own frontier past what {@code TTL 3 DAYS} keeps.
     * <p>
     * The first four rows sit on consecutive days and stay whole under the TTL, so a restart taken
     * after them resumes off a root it can prove and derives a skip-write floor of four. The fifth
     * row jumps to 1970-01-07, which is three days past 1970-01-04's partition ceiling: the turn
     * that commits it evicts the first three days at once and leaves the table holding fewer rows
     * than the floor names, with 1970-01-04 - an ordinal below the floor - surviving to the end.
     * <p>
     * The last three rows share that day and sit in three different base hours. The base is
     * hour-partitioned and the view day-partitioned for exactly that reason: a turn can open a
     * fresh base partition mid-flight, which is where a read fault reaches a turn that has already
     * fed a row, while the view's frontier moves by two hours rather than by a day.
     */
    private void createResumeFloorSeedBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("""
                INSERT INTO base (ts, sym, x) VALUES
                ('1970-01-01T00:00:00.000000Z', 'a', 1),
                ('1970-01-02T00:00:00.000000Z', 'a', 2),
                ('1970-01-03T00:00:00.000000Z', 'a', 3),
                ('1970-01-04T00:00:00.000000Z', 'a', 4),
                ('1970-01-07T00:00:00.000000Z', 'a', 5),
                ('1970-01-07T01:00:00.000000Z', 'a', 6),
                ('1970-01-07T01:30:00.000000Z', 'a', 7),
                ('1970-01-07T02:00:00.000000Z', 'a', 8)""");
        drainWalQueue();
        setCurrentMicros(ts("1970-02-01T00:00:00.000000Z"));
    }

    /**
     * Five daily base rows under one symbol, and a wall clock parked above them so TTL measures
     * partition age against the live view's own frontier rather than against the clock.
     */
    /**
     * {@link #createSeedBase}'s history over seven days rather than five, so a sweep under
     * {@code TTL 2 DAYS} still has turns left after its first eviction - which is what a mid-sweep
     * {@code SET TTL 0} needs in order to change the outcome at all.
     */
    private void createSevenDaySeedBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base (ts, sym, x) VALUES
                ('1970-01-01T00:00:00.000000Z', 'a', 1),
                ('1970-01-02T00:00:00.000000Z', 'a', 2),
                ('1970-01-03T00:00:00.000000Z', 'a', 3),
                ('1970-01-04T00:00:00.000000Z', 'a', 4),
                ('1970-01-05T00:00:00.000000Z', 'a', 5),
                ('1970-01-06T00:00:00.000000Z', 'a', 6),
                ('1970-01-07T00:00:00.000000Z', 'a', 7)""");
        drainWalQueue();
        setCurrentMicros(ts("1970-02-01T00:00:00.000000Z"));
    }

    /**
     * The state every case of the seed reset's crash matrix starts from: {@link #createSeedBase}'s
     * five days seeded under {@code TTL 2 DAYS}, one row per turn, and the process ended right after
     * the sweep's own commit of 1970-01-04 evicted 1970-01-01. The table holds 1970-01-02 to
     * 1970-01-04 under a live retention marker, so the resume this restart leaves behind cannot
     * prove the durable output, and the next seed turn is the reset: a full-range replacement that
     * carries 1970-01-01, the first re-swept row.
     */
    private void interruptSeedAfterItsFirstEviction() throws Exception {
        createSeedBase();
        createSeedView("TTL 2 DAYS ");
        final TableToken lvToken = engine.verifyTableName("lv");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedTurnsUntilEviction(job);
        }
        Assert.assertEquals("the sweep must have written four rows and lost one", 3, lvRowCount(lvToken));
        assertRetentionMarker(lvToken, true);
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * What a seed {@link #interruptSeedAfterItsFirstEviction} left behind finishes as, however many
     * faults, resets and restarts it took to get there: exactly what an uninterrupted seed produces,
     * with the lifetime counter, the ladder and both pieces of reconciliation evidence settled.
     */
    private void assertSeedFinishedAfterItsFirstEviction(LiveViewInstance instance) throws Exception {
        assertSurvivingSeedRows();
        Assert.assertEquals(
                "the seed must have completed",
                LiveViewState.SEED_STATE_ACTIVE,
                instance.getStateReader().getSeedState()
        );
        Assert.assertEquals("the evicted rows must leave the lifetime counter", 3, instance.getLvRowsTotal());
        Assert.assertFalse(instance.hasPendingPartitionRemovals());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getPendingReplacementLvSeqTxn());
        assertRetentionMarker(engine.verifyTableName("lv"), false);
        assertLadder(instance, ts("1970-01-05"), 3);
        assertQuery("SELECT checkpoint_row_count_mismatches FROM live_views()")
                .noLeakCheck().noRandomAccess().returns("checkpoint_row_count_mismatches\n0\n");
    }

    private void createSeedBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base (ts, sym, x) VALUES
                ('1970-01-01T00:00:00.000000Z', 'a', 1),
                ('1970-01-02T00:00:00.000000Z', 'a', 2),
                ('1970-01-03T00:00:00.000000Z', 'a', 3),
                ('1970-01-04T00:00:00.000000Z', 'a', 4),
                ('1970-01-05T00:00:00.000000Z', 'a', 5)""");
        drainWalQueue();
        setCurrentMicros(ts("1970-02-01T00:00:00.000000Z"));
    }

    /**
     * A view over {@link #createSeedBase}'s history that seeds the lot: START FROM BEGINNING, so
     * every base row is admitted and the sweep's cursor offset and output ordinal are both counted
     * from the first base row. {@code ttlClause} is either empty or a full {@code TTL n UNIT }
     * clause, trailing space included.
     */
    private void createSeedView(String ttlClause) throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY DAY " + ttlClause + "START FROM BEGINNING AS "
                + "(SELECT ts, x, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }

    /**
     * Drives seed turns until {@code condition} holds, leaving the view SEEDING. Fails when the
     * sweep completes first, which would make every assertion after it vacuous.
     */
    private LiveViewInstance driveSeedTurnsUntil(LiveViewRefreshJob job, BooleanSupplier condition, String failure) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        for (int i = 0; i < SEED_COMPLETION_PASSES; i++) {
            if (condition.getAsBoolean()) {
                return instance;
            }
            Assert.assertEquals(
                    "the sweep completed first: " + failure,
                    LiveViewState.SEED_STATE_SEEDING,
                    instance.getStateReader().getSeedState()
            );
            // One turn per pass, not drainJob's up-to-64: the states these callers stop on are
            // mid-sweep ones, and a whole sweep inside one pass would run straight past them.
            job.run();
            drainWalQueue();
        }
        Assert.fail(failure);
        return instance;
    }

    /**
     * Drives seed turns until one of the sweep's own commits has evicted a partition under TTL.
     */
    private LiveViewInstance driveSeedTurnsUntilEviction(LiveViewRefreshJob job) {
        return driveSeedTurnsUntil(
                job,
                () -> engine.getLiveViewRegistry().getViewInstance("lv").hasPendingPartitionRemovals(),
                "the seed never evicted a partition"
        );
    }

    private long lvRowCount(TableToken lvToken) {
        try (TableReader reader = engine.getReader(lvToken)) {
            return reader.size();
        }
    }

    /**
     * Base table with DEDUP keys, so the view it backs is coupled: no un-flushed lead, an inline
     * apply every cycle, and the tier fed only by the cycle's disk-subset publish. Which of the two
     * coupled cycles runs depends on the range - a provably clean one takes {@code incrementalRefresh}'s
     * raw-WAL drain, a range that deduped takes {@code drainAppliedBase}.
     */
    private void createDedupBaseAndView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                "DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY DAY START FROM NOW AS " +
                "(SELECT ts, x, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }

    /**
     * An hourly base and a view holding its recent hours in RAM. The {@code IN MEMORY} window is
     * wider than the hour TTL keeps, so an eviction lands INSIDE the overlap band rather than below
     * it, and {@code rn} is a running ordinal over one symbol, so a row served twice or served from
     * a stale band shows up as a repeated or shifted value rather than only as a row count.
     */
    private void createHourlyBandedView(String inMemory, String ttlClause) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY " + inMemory + " PARTITION BY HOUR " + ttlClause +
                "START FROM NOW AS (SELECT ts, x, count(*) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }

    /**
     * The hourly base and view the out-of-order repair cases run over: a 30-second {@code RANGE}
     * frame, narrow enough that a correction a few seconds into an hour converges well below the
     * frontier, so the repair localizes to a finite {@code [R, H)} and splices.
     */
    private void createHourlyRangeView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY HOUR START FROM NOW AS " +
                "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base)");
    }

    /**
     * One commit - and so one logical root - per row over {@link #createHourlyRangeView}: hour 01
     * holds one row, hour 02 three, and hour 03, the active partition, one. A correction at
     * 02:00:15 then re-versions the roots at 02:00:20 and 02:00:30, keeps 02:00:10 below it and
     * 03:00:10 above it, and leaves hour 01 as a partition a {@code DROP} can take on either side
     * of the repair's interval.
     */
    private LiveViewInstance flushHourlyRepairHistory(LiveViewRefreshJob job) throws Exception {
        flushOneRow(job, "2026-01-01T01:00:10.000000Z", 1, 1);
        flushOneRow(job, "2026-01-01T02:00:10.000000Z", 2, 2);
        flushOneRow(job, "2026-01-01T02:00:20.000000Z", 3, 3);
        flushOneRow(job, "2026-01-01T02:00:30.000000Z", 4, 4);
        flushOneRow(job, "2026-01-01T03:00:10.000000Z", 5, 5);
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        assertLadder(
                instance,
                ts("2026-01-01T01:00:10.000000Z"), 1,
                ts("2026-01-01T02:00:10.000000Z"), 2,
                ts("2026-01-01T02:00:20.000000Z"), 3,
                ts("2026-01-01T02:00:30.000000Z"), 4,
                ts("2026-01-01T03:00:10.000000Z"), 5
        );
        return instance;
    }

    /**
     * The anchored per-account running sum the keyed repair routes run over, seeded with
     * 2026-01-02 and 2026-01-03: four accounts, one row each per hour from 00 to 09, at minute
     * {@code 10 * account}. The base's account column carries the posting index a keyed replay
     * follows, the view's own table carries {@code (created_at, account_id)} as the dedup keys a
     * sparse publication upserts on - which needs the sparse-publication switch on before the
     * CREATE - and the view inherits the base's hourly partitioning, so a {@code DROP} can take
     * one hour of one day on its own. The daily anchor makes each day a segment of its own, so a
     * correction inside one day re-derives nothing outside it.
     */
    private void createKeyedView() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 4, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 3; day++) {
            for (int hour = 0; hour < 10; hour++) {
                appendKeyedHour(rows, day, hour);
            }
        }
        execute("INSERT INTO tx VALUES " + rows);
        drainWalQueue();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum FROM tx "
                + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    /**
     * Drives 2026-01-04 over {@link #createKeyedView} in order, one commit per hour from 00 to 09,
     * so the one-row cadence seals a root at every hour's last row. That root below a correction
     * inside the day is what lets the plan resume from an anchor; without one it takes the
     * head-miss executor's cold keyed route instead.
     */
    private void openKeyedDayAboveRoots(LiveViewRefreshJob job) throws Exception {
        for (int hour = 0; hour < 10; hour++) {
            final StringBuilder rows = new StringBuilder();
            appendKeyedHour(rows, 4, hour);
            commitKeyed(rows.toString(), job);
        }
    }

    /**
     * Drives the worker that parked a repair until it finishes it, one clock step and one
     * notification pass at a time - the drive a parked repair's owner takes between its turns.
     */
    private void driveParkedRepairToCompletion(LiveViewRefreshJob job, LiveViewInstance instance) {
        for (int turn = 0; turn < REFRESH_QUIESCENCE_PASSES && instance.getSuspendedRepair() != null; turn++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            job.processNotificationsForTest();
            drainWalQueue();
        }
        Assert.assertNull("the owner must finish the repair it parked", instance.getSuspendedRepair());
    }

    private void commitKeyed(String values, LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO tx VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Asserts {@link #createKeyedView}'s view holds exactly what a from-base recompute of its
     * SELECT computes, minus the rows {@code droppedPredicate} names. The filter runs over the
     * computed output, not over the base the window reads, so a surviving row still carries the
     * running sum that counted the rows a {@code DROP} took from the view.
     */
    private void assertKeyedViewMatchesRecomputeExcept(String droppedPredicate) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "SELECT created_at, account_id, "
                + "sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum "
                + "FROM (SELECT created_at, account_id, amount, " + bucket + " AS bucket FROM tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT * FROM (" + recompute + ") WHERE NOT (" + droppedPredicate + ")) ORDER BY 2, 1, 3",
                "(lv) ORDER BY 2, 1, 3",
                LOG,
                true
        );
    }

    /**
     * Holds every root of {@link #createKeyedView}'s ladder to the number of rows its table holds
     * at or below the root's own timestamp - the identity a resume anchoring on that root reads.
     */
    private void assertKeyedLadderCountsRowsAtOrBelowEachBoundary(LiveViewInstance instance) throws Exception {
        final LongList ladder = snapshotCheckpointLadder(instance);
        Assert.assertTrue("the view must hold a ladder to check", ladder.size() > 0);
        for (int i = 0, n = ladder.size() / 2; i < n; i++) {
            final long maxTimestamp = ladder.getQuick(i * 2);
            try (
                    RecordCursorFactory factory = select("SELECT count() FROM lv WHERE created_at <= " + maxTimestamp + "::timestamp");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(
                        "root " + i + " at " + maxTimestamp + " must count the rows at or below it",
                        cursor.getRecord().getLong(0),
                        ladder.getQuick(i * 2 + 1)
                );
            }
        }
    }

    private void createBaseAndView(String partitionByClause) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s " + partitionByClause + " START FROM NOW AS " +
                "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }
}
