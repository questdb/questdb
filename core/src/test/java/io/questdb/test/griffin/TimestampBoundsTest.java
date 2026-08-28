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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TimestampBoundsTest extends AbstractCairoTest {
    private static final String NANOS_OUT_OF_BOUNDS =
            "designated timestamp_ns before 1970-01-01 and beyond 2261-12-31 23:59:59.999999999 is not allowed";
    // 2262-01-01T00:00:00Z: the first nanosecond of the band a pre-fix build accepted and head rejects
    private static final long OUT_OF_BOUNDS_NANO = CommonUtils.MAX_TIMESTAMP + 1;
    // 2262-02-01T00:00:00Z: a legacy value 31 days into that band, chosen so that a forward shift of
    // OFFSET_PRUNING_STRIDE_DAYS wraps it past Long.MAX_VALUE while staying inside the headroom a
    // CommonUtils.MAX_TIMESTAMP ceiling would leave
    private static final long LEGACY_BAND_NANO = OUT_OF_BOUNDS_NANO + 31 * Nanos.DAY_NANOS;
    // 80 days: above the ~70 days that wrap LEGACY_BAND_NANO, below the ~101 days of headroom
    // Long.MAX_VALUE - CommonUtils.MAX_TIMESTAMP leaves
    private static final int OFFSET_PRUNING_STRIDE_DAYS = 80;
    // Longer than the FLUSH EVERY 100ms the live view declares, so every pass crosses a flush
    // deadline instead of leaving the view waiting on the clock.
    private static final long LIVE_VIEW_CLOCK_ADVANCE_MICROS = 150_000;
    // Ten times the five consecutive failures the default flush retry budget allows, so a view that
    // stops invalidating fails the assertion rather than the pass bound.
    private static final int LIVE_VIEW_REFRESH_PASSES = 50;
    // the same instant in micros, where it is an ordinary legal value: the micros ceiling is 9999-12-31
    private static final long POST_2261_MICRO = CommonUtils.MAX_TIMESTAMP / 1_000 + 1;

    private final boolean walEnabled;

    public TimestampBoundsTest(boolean walEnabled) {
        this.walEnabled = walEnabled;
    }

    @Parameterized.Parameters(name = "WAL={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, true);
        engine.getDependentViewGraph().clear();
    }

    @Test
    public void testDesignatedNanosTimestampBoundsNonPartitioned() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts)");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900000000Z')")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + (CommonUtils.MAX_TIMESTAMP + 1) + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
        });
    }

    @Test
    public void testDesignatedNanosTimestampBoundsPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900000000Z')")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + (CommonUtils.MAX_TIMESTAMP + 1) + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
        });
    }

    @Test
    public void testDesignatedNanosTimestampBoundsWithSwitchPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + 1L + ")");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900000000Z')")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + (CommonUtils.MAX_TIMESTAMP + 1) + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
        });
    }

    /**
     * The same rejection needs no legacy data at all. A micros table legally holds timestamps well
     * past 2262 - its ceiling is 9999-12-31 - and copying one into a timestamp_ns designated column
     * multiplies it by 1000, which lands in the rejected band. A micros table with any row after
     * 2262-01-01 therefore cannot be copied wholesale into a nano table.
     * <p>
     * Both statements below are worth running, because the multiplication happens in a different
     * place in each. The {@code INSERT INTO ... SELECT} leaves the cursor in micros, so the copier
     * converts on the way into {@code newRow()}; the CTAS carries an explicit
     * {@code ts::TIMESTAMP_NS}, so the projection has already multiplied before the copier sees the
     * value. Either way the writer's {@code newRow()} receives the same out-of-range nano timestamp
     * and rejects it.
     */
    @Test
    public void testDesignatedNanosTimestampCopyForwardFromMicrosRejectsPost2261Row() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quebec (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY YEAR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO quebec VALUES (" + (POST_2261_MICRO - 1) + ", 1), (" + POST_2261_MICRO + ", 2)");
            execute("CREATE TABLE tango (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY YEAR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            drainWalQueue();

            assertQuery("INSERT INTO tango SELECT ts, x FROM quebec")
                    .fails(0, NANOS_OUT_OF_BOUNDS);
            assertQuery("CREATE TABLE hopper AS (SELECT ts::TIMESTAMP_NS ts, x FROM quebec) TIMESTAMP(ts) PARTITION BY YEAR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL")
                    .fails(13, NANOS_OUT_OF_BOUNDS);

            // the last micros value that still converts into the legal nano range copies over
            execute("INSERT INTO tango SELECT ts, x FROM quebec WHERE ts < " + POST_2261_MICRO);
            drainWalQueue();
            assertQuery("SELECT * FROM tango").timestamp("ts").expectSize().returns("""
                    ts\tx
                    2261-12-31T23:59:59.999999000Z\t1
                    """);
        });
    }

    /**
     * A timestamp_ns table written before the ceiling was enforced can hold a designated timestamp in
     * the band {@code (2261-12-31 23:59:59.999999999, ~2262-04-11]}. {@code TableWriter.newRow()} and
     * {@code WalWriter.newRow()} now reject that value, so every statement that copies such a row
     * forward fails. Both the {@code INSERT INTO ... SELECT} and the CTAS below hand {@code newRow()}
     * the record's timestamp as it stands: source and destination are both timestamp_ns, so nothing
     * converts in between. A materialized view over such a table stops refreshing for the same
     * reason - its refresh checks the timestamp against the replace range it is refreshing, never
     * against the nano ceiling.
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowInvalidatesMatView()} drives that refresh and
     * pins the invalidation it leaves behind.
     * <p>
     * That is also why the source here is a plain, non-designated TIMESTAMP_NS column, which has no
     * ceiling: it is the only supported way to hold the value. The destination writer receives the
     * identical long and cannot tell the two sources apart.
     */
    @Test
    public void testDesignatedNanosTimestampCopyForwardRejectsOutOfBoundsRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE legacy (ts TIMESTAMP_NS, x LONG)");
            execute("INSERT INTO legacy VALUES (" + CommonUtils.MAX_TIMESTAMP + ", 1), (" + OUT_OF_BOUNDS_NANO + ", 2)");
            execute("CREATE TABLE tango (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY YEAR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");

            assertQuery("INSERT INTO tango SELECT ts, x FROM legacy")
                    .fails(0, NANOS_OUT_OF_BOUNDS);
            drainWalQueue();
            // the legal row that preceded the rejected one does not survive either: the statement
            // is all-or-nothing, and the destination table is left usable
            assertQuery("SELECT count() FROM tango").noRandomAccess().expectSize().returns("""
                    count
                    0
                    """);

            assertQuery("CREATE TABLE hopper AS (SELECT ts, x FROM legacy) TIMESTAMP(ts) PARTITION BY YEAR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL")
                    .fails(13, NANOS_OUT_OF_BOUNDS);
            // the half-built destination of the failed CTAS is removed
            assertQuery("SELECT * FROM hopper").fails(14, "table does not exist");

            // the operator workaround: filter the out-of-range rows out of the copy
            execute("INSERT INTO tango SELECT ts, x FROM legacy WHERE ts <= " + CommonUtils.MAX_TIMESTAMP);
            drainWalQueue();
            assertQuery("SELECT * FROM tango").timestamp("ts").expectSize().returns("""
                    ts\tx
                    2261-12-31T23:59:59.999999999Z\t1
                    """);
        });
    }

    /**
     * The rejected row must not leave the writer distressed or the partition bookkeeping damaged:
     * before the bound was enforced, a batch that ended past the ceiling threw
     * {@code ArrayIndexOutOfBoundsException} out of {@code TxWriter} and killed the writer.
     */
    @Test
    public void testDesignatedNanosTimestampOutOfBoundsKeepsTableUsable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES ('2024-01-01T00:00:00.000000000Z')");
            assertQuery("INSERT INTO tango VALUES (" + (Long.MAX_VALUE - 1) + "), (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            execute("INSERT INTO tango VALUES ('2024-01-02T00:00:00.000000000Z')");
            drainWalQueue();
            assertQuery("SELECT * FROM tango").timestamp("ts").expectSize().returns("""
                    ts
                    2024-01-01T00:00:00.000000000Z
                    2024-01-02T00:00:00.000000000Z
                    """);
            assertQuery("SELECT count() FROM tango").noRandomAccess().expectSize().returns("""
                    count
                    2
                    """);
        });
    }

    /**
     * The live-view twin of
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowInvalidatesMatView()}: a live view whose base
     * table a pre-fix build let an out-of-range row into stops refreshing and goes durably invalid.
     * This test drives the view to ACTIVE over an empty base first, so the row arrives through the
     * refresh drain, faults on the flush that materialises it, and charges one unit of the flush
     * retry budget per retry until the budget runs out.
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowInvalidatesSeedingLiveView()} covers the
     * other leg, where the row predates the view and the seed sweep meets it instead. The refresh
     * path hands base-derived timestamps to {@code WalWriter.newRow(long)} at several further sites
     * that neither test reaches, and that residual gap is declared rather than implied.
     * <p>
     * The invalidation reason is the generic {@code "flush retry budget exhausted"}, not the ceiling
     * message the mat-view sibling records: the live view path reports the budget rather than the
     * error that spent it, so an operator reading {@code live_views()} does not learn which row broke
     * the view. That is the observable state, so it is what this test pins.
     * <p>
     * A live view requires a WAL base table, so the {@code Assume} skips the WAL=false half of the
     * matrix. The WAL segment rewrite and the DAY partitioning are here for the reasons
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowInvalidatesMatView()} spells out, and the
     * test lives here rather than in the {@code cairo/lv} suite because that fixture - the rewrite,
     * the two nano constants and the mat-view sibling it contrasts with - is here already.
     * <p>
     * Every {@code assertQuery} this test runs, here and in
     * {@link #assertLiveViewInvalidatedByLegacyNanoRow()}, asks for {@code noLeakCheck()}: the
     * leak-checking form opens with {@code engine.clear()}, which wipes both the metadata cache
     * {@code CREATE LIVE VIEW} resolves its base table through and the live view registry
     * {@code live_views()} reads.
     */
    @Test
    public void testDesignatedNanosTimestampOutOfBoundsRowInvalidatesLiveView() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            // Pin the clock below the data. The live view's lower bound is its START FROM boundary,
            // and the refresh path drops base rows below it.
            setCurrentMicros(0L);
            execute("CREATE TABLE base (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A live view requires a WAL base table and an explicit START FROM clause. PARTITION BY
            // DAY is as deliberate here as in the mat-view sibling, and for the same reason.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '1970-01-01T00:00:00.000000000Z' AS "
                    + "SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn "
                    + "FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("live view 'lv' is not registered", instance);
                // The base is empty at CREATE, so the seed sweep has nothing to sweep and the view
                // reaches the ACTIVE state the drain path runs in. Creating the view over a base that
                // already holds the row takes the seed sweep instead - that is the sibling
                // testDesignatedNanosTimestampOutOfBoundsRowInvalidatesSeedingLiveView().
                driveLiveViewSeedToCompletion(job, instance);

                // Now plant the legacy row, exactly as the mat-view sibling does: write the last
                // legal nanosecond, rewrite it in the WAL segment on disk, and let the apply job
                // import a value no writer on this build would have accepted.
                execute("INSERT INTO base VALUES (" + CommonUtils.MAX_TIMESTAMP + ", 1)");
                // close the WAL writer so the segment on disk is complete before the rewrite
                engine.releaseInactive();
                rewriteWalSegmentDesignatedTimestamp("base", CommonUtils.MAX_TIMESTAMP, OUT_OF_BOUNDS_NANO);
                drainWalQueue();
                assertQuery("SELECT ts, x FROM base").noLeakCheck().timestamp("ts").expectSize().returns("""
                        ts\tx
                        2262-01-01T00:00:00.000000000Z\t1
                        """);

                driveLiveViewToInvalidation(job, instance);
            }

            assertLiveViewInvalidatedByLegacyNanoRow();
        });
    }

    /**
     * A materialized view over a base table that a pre-fix build let an out-of-range row into stops
     * refreshing and goes durably invalid. {@code MatViewRefreshJob} hands every result row's
     * timestamp straight to {@code WalWriter.newRow(long)}, which rejects it. That failure is none
     * of the kinds the refresh job defers and retries, so it takes the fail path, which resets the
     * view's WAL state with {@code invalid=true} and cascades to dependent views.
     * <p>
     * The seam: {@code WalWriter.newRow()} and {@code TableWriter.newRow()} both validate, so no
     * statement on this build can write the row. The WAL apply job copies segment columns in bulk and
     * never re-validates, so the test writes the last legal nanosecond, rewrites it in the WAL
     * segment on disk, and lets the apply job import the result. That reproduces exactly the on-disk
     * state a build without the bound produced - an operator upgrading onto this build has such
     * tables, which is why {@code NanosTimestampDriver.getMaxDesignatedTimestamp()} still reports
     * {@code Long.MAX_VALUE}.
     * <p>
     * Both tables are partitioned by DAY on purpose. A nanosecond YEAR partition holding 2262 has no
     * representable ceiling - 2263-01-01 overflows a long - and {@code TableWriter}'s O3 path then
     * spins over partition tasks and drops the row instead of applying it.
     * <p>
     * {@code CreateMatViewOperationImpl} rejects a non-WAL base table ("base table has to be WAL
     * enabled"), so both DDLs name WAL outright and the {@code Assume} skips the WAL=false half of
     * the matrix rather than running a byte-identical body a second time.
     */
    @Test
    public void testDesignatedNanosTimestampOutOfBoundsRowInvalidatesMatView() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES (" + CommonUtils.MAX_TIMESTAMP + ", 1)");
            // close the WAL writer so the segment on disk is complete before the rewrite
            engine.releaseInactive();
            rewriteWalSegmentDesignatedTimestamp("base", CommonUtils.MAX_TIMESTAMP, OUT_OF_BOUNDS_NANO);
            drainWalQueue();

            // the base table now holds a designated timestamp no writer on this build accepts
            assertQuery("SELECT ts, x FROM base").timestamp("ts").expectSize().returns("""
                    ts\tx
                    2262-01-01T00:00:00.000000000Z\t1
                    """);

            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, sum(x) AS x FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            drainPurgeJob();

            // the refresh fails on the nano ceiling and the failure is durable, not retried
            assertQuery("SELECT view_name, base_table_name, view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            mv\tbase\tinvalid\t[-1]: %s
                            """.formatted(NANOS_OUT_OF_BOUNDS));
            // nothing reached the view: the ceiling rejects the row before the copier runs
            assertQuery("SELECT * FROM mv").timestamp("ts").expectSize().returns("""
                    ts\tx
                    """);
        });
    }

    /**
     * A live view CREATEd over a base table that ALREADY holds a legacy out-of-range nano row faults
     * inside its seed sweep rather than its drain, and must invalidate rather than retry forever.
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowInvalidatesLiveView()} covers the other leg,
     * where the view reaches ACTIVE over an empty base first and the row arrives through the drain.
     * <p>
     * The seeding leg used to leave the refresh failure path before recording anything: neither the
     * count budget nor the wall-clock duration budget ever armed, the view sat at {@code seeding}
     * with an empty {@code invalidation_reason} forever, and every refresh call reported work, so a
     * production worker never backed off. {@link #assertLiveViewRefreshJobIdle(LiveViewRefreshJob)}
     * pins the second half of that contract: invalidating the view is what lets the refresh scan skip
     * it, which is what lets {@code job.run()} report no work and a worker nap.
     * <p>
     * The fixture - WAL-only, DAY partitioning, the segment rewrite, the clock every refresh cycle
     * advances past the view's {@code FLUSH EVERY} interval - is the sibling's, for the reasons its
     * javadoc gives, the {@code noLeakCheck()} on every {@code assertQuery} included.
     */
    @Test
    public void testDesignatedNanosTimestampOutOfBoundsRowInvalidatesSeedingLiveView() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            // Pin the clock below the data, as the sibling does: the view's START FROM boundary is
            // its lower bound, and the seed sweep drops base rows below it.
            setCurrentMicros(0L);
            execute("CREATE TABLE base (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Plant the legacy row BEFORE the view exists, so the seed sweep - not the drain - meets
            // it: write the last legal nanosecond, rewrite it in the WAL segment on disk, and let the
            // apply job import a value no writer on this build would have accepted.
            execute("INSERT INTO base VALUES (" + CommonUtils.MAX_TIMESTAMP + ", 1)");
            // close the WAL writer so the segment on disk is complete before the rewrite
            engine.releaseInactive();
            rewriteWalSegmentDesignatedTimestamp("base", CommonUtils.MAX_TIMESTAMP, OUT_OF_BOUNDS_NANO);
            drainWalQueue();
            assertQuery("SELECT ts, x FROM base").noLeakCheck().timestamp("ts").expectSize().returns("""
                    ts\tx
                    2262-01-01T00:00:00.000000000Z\t1
                    """);

            // A live view requires a WAL base table and an explicit START FROM clause. PARTITION BY
            // DAY is as deliberate here as in the mat-view sibling, and for the same reason.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '1970-01-01T00:00:00.000000000Z' AS "
                    + "SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn "
                    + "FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("live view 'lv' is not registered", instance);
                // CREATE over a base that already holds committed history lands the view in SEEDING,
                // which is the whole point here: this test measures the seed sweep's newRow, and a
                // view that started ACTIVE would measure the drain's instead.
                Assert.assertEquals(
                        "live view 'lv' must start out SEEDING",
                        LiveViewState.SEED_STATE_SEEDING,
                        instance.getStateReader().getSeedState()
                );
                driveLiveViewToInvalidation(job, instance);
                assertLiveViewRefreshJobIdle(job);
            }

            assertLiveViewInvalidatedByLegacyNanoRow();
        });
    }

    /**
     * A legacy row above the nano ceiling must survive a predicate the interval scan is free to
     * prune, which is why {@code NanosTimestampDriver.getMaxDesignatedTimestamp()} reports
     * {@code Long.MAX_VALUE} rather than {@code CommonUtils.MAX_TIMESTAMP}. That value is the
     * ceiling on the input of a forward {@code dateadd} shift, and the shift can be declared unable
     * to wrap past {@code Long.MAX_VALUE} only when the ceiling bounds every timestamp the table can
     * actually hold. Reporting the long ceiling keeps every positive shift wrapping, so the
     * predicate stays a row filter and the row is scanned rather than pruned.
     * <p>
     * The seeded row sits 31 days above the ceiling, so the 80-day shift carries it past
     * {@code Long.MAX_VALUE} and wraps it to a large negative value, well below the bound.
     * Tightening the reported ceiling to {@code CommonUtils.MAX_TIMESTAMP} leaves ~101 days of
     * headroom, declares the 80-day shift incapable of wrapping, prunes the row away and answers
     * with an empty set.
     * <p>
     * This spelling reads the ceiling through
     * {@code MonotonicTimestampFunction.shiftInputCeiling()};
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowSurvivesDateaddPushdown()} covers the other
     * consumer.
     */
    @Test
    public void testDesignatedNanosTimestampOutOfBoundsRowSurvivesDateaddFilter() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            seedLegacyBandRow();
            assertQuery("SELECT x FROM legacy "
                    + "WHERE dateadd('d', " + OFFSET_PRUNING_STRIDE_DAYS + ", ts) < '2020-01-01T00:00:00Z'")
                    .withPlanContaining("filter: dateadd('d'," + OFFSET_PRUNING_STRIDE_DAYS + ",ts)<")
                    .returns("""
                            x
                            1
                            """);
        });
    }

    /**
     * The pushed-down twin of
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowSurvivesDateaddFilter()}. The sub-query
     * projection makes {@code SqlOptimiser} wrap the predicate in {@code and_offset} and push it onto
     * the base table, and {@code WhereClauseParser.analyzeAndOffset()} then reads the driver ceiling
     * itself rather than going through a {@code MonotonicTimestampFunction}. The plan assertion pins
     * the decline: the predicate has to stay a residual filter instead of becoming an interval scan
     * that never opens the 2262 partition.
     */
    @Test
    public void testDesignatedNanosTimestampOutOfBoundsRowSurvivesDateaddPushdown() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            seedLegacyBandRow();
            assertQuery("SELECT x FROM (SELECT dateadd('d', " + OFFSET_PRUNING_STRIDE_DAYS + ", ts) tt, x FROM legacy) "
                    + "WHERE tt < '2020-01-01T00:00:00Z'")
                    .withPlanContaining("filter: dateadd('d'," + OFFSET_PRUNING_STRIDE_DAYS + ",ts)<")
                    .returns("""
                            x
                            1
                            """);
        });
    }

    /**
     * The last legal nanosecond, one below the ceiling the error message names, must still be
     * storable and readable back.
     */
    @Test
    public void testDesignatedNanosTimestampUpperBoundIsInclusive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + CommonUtils.MAX_TIMESTAMP + ")");
            drainWalQueue();
            assertQuery("SELECT * FROM tango").timestamp("ts").expectSize().returns("""
                    ts
                    2261-12-31T23:59:59.999999999Z
                    """);
        });
    }

    @Test
    public void testDesignatedTimestampBoundsNonPartitioned() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts)");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")")
                    .fails(26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testDesignatedTimestampBoundsPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")")
                    .fails(26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testDesignatedTimestampBoundsWithSwitchPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + 1L + ")");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")")
                    .fails(26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testNanosTimestampBoundsNotDesignated() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")");
            execute("INSERT INTO tango VALUES (" + -1L + ")");
        });
    }

    @Test
    public void testTimestampBoundsNotDesignated() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP)");
            execute("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")");
            execute("INSERT INTO tango VALUES (" + -1L + ")");
            execute("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')");
        });
    }

    /**
     * Asserts {@code job} reports no work on every one of {@link #LIVE_VIEW_REFRESH_PASSES}
     * consecutive calls, each of them across a fresh flush deadline. A refresh cycle that attempts a
     * view reports work whether or not it succeeded, and a worker pool reads that as "reschedule me
     * immediately", so a view whose refresh fails without ever terminating burns one worker at full
     * tilt forever. Invalidation is what ends it: every refresh entry path skips an invalid view, so
     * the scan finds nothing to do and the worker naps.
     * <p>
     * A single idle call would not show that. The job has to keep answering "no work" as the clock
     * moves past the view's {@code FLUSH EVERY} interval, so every pass advances the clock the way
     * {@link #liveViewRefreshCycle(LiveViewRefreshJob)} does.
     */
    private static void assertLiveViewRefreshJobIdle(LiveViewRefreshJob job) {
        for (int i = 0; i < LIVE_VIEW_REFRESH_PASSES; i++) {
            setCurrentMicros(currentMicros + LIVE_VIEW_CLOCK_ADVANCE_MICROS);
            Assert.assertFalse(
                    "live view refresh job reported work on call " + (i + 1) + " of " + LIVE_VIEW_REFRESH_PASSES
                            + " consecutive calls: a worker pool never backs off from that",
                    job.run()
            );
        }
    }

    /**
     * Drives {@code job} until {@code instance} leaves the SEEDING state. Fails rather than falls out
     * of the loop: a view still seeding would take the seed sweep's {@code newRow}, not the drain
     * path's, so the caller would be measuring a different site than it thinks.
     */
    private static void driveLiveViewSeedToCompletion(LiveViewRefreshJob job, LiveViewInstance instance) {
        for (int i = 0; i < LIVE_VIEW_REFRESH_PASSES; i++) {
            if (instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_SEEDING) {
                return;
            }
            liveViewRefreshCycle(job);
        }
        Assert.fail("live view is still SEEDING after " + LIVE_VIEW_REFRESH_PASSES + " refresh cycles");
    }

    /**
     * Drives {@code job} until {@code instance} goes invalid. Fails if it does not: a view that keeps
     * failing without ever spending its flush retry budget re-runs the whole refresh every cycle
     * forever, which is the outcome this bound exists to catch.
     */
    private static void driveLiveViewToInvalidation(LiveViewRefreshJob job, LiveViewInstance instance) {
        for (int i = 0; i < LIVE_VIEW_REFRESH_PASSES; i++) {
            liveViewRefreshCycle(job);
            if (instance.isInvalid()) {
                return;
            }
        }
        Assert.fail("live view did not invalidate within " + LIVE_VIEW_REFRESH_PASSES
                + " refresh cycles [refreshFaults=" + instance.getRefreshFaultCount()
                + "]: no fault at all means the refresh accepted the out-of-range row, and a rising"
                + " count means it keeps retrying a failure it never charges its flush retry budget for");
    }

    /**
     * Advances the clock and runs {@code job} for one refresh cycle, then applies whatever the cycle
     * wrote to the live view's own WAL. Mirrors the {@code refreshCycle} idiom the {@code cairo/lv}
     * suite hand-drives its refresh job with.
     */
    private static void liveViewRefreshCycle(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + LIVE_VIEW_CLOCK_ADVANCE_MICROS);
        for (int i = 0; i < 64 && job.run(); i++) {
            // run the job out until it reports no further work for this cycle
        }
        drainWalQueue();
    }

    /**
     * Overwrites {@code expected} with {@code replacement} at every offset in {@code offsets} of the
     * file {@code path} points at. Each offset is checked to really hold {@code expected} first, so a
     * change to the on-disk layout fails the test instead of silently patching the wrong bytes.
     */
    private static void rewriteLongs(FilesFacade ff, Path path, long expected, long replacement, long... offsets) {
        final long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
        try {
            final long addr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (long offset : offsets) {
                    Assert.assertEquals(Long.BYTES, ff.read(fd, addr, Long.BYTES, offset));
                    Assert.assertEquals("unexpected value in " + path + " at offset " + offset, expected, Unsafe.getLong(addr));
                    Unsafe.putLong(addr, replacement);
                    Assert.assertEquals(Long.BYTES, ff.write(fd, addr, Long.BYTES, offset));
                }
            } finally {
                Unsafe.free(addr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Replaces the designated timestamp of the first row of the first WAL segment of {@code tableName},
     * so that the pending transaction carries a value the WAL apply job imports but no writer on this
     * build would have accepted.
     */
    private static void rewriteWalSegmentDesignatedTimestamp(String tableName, long expected, long replacement) {
        // A WAL segment stores the designated timestamp as a LONG128 (timestamp, row id) pair, so the
        // first row's timestamp is the first 8 bytes of ts.d.
        final long timestampColumnOffset = 0;
        // The segment's _event file repeats that timestamp as the transaction's min and max. TableWriter
        // takes the table's own min/max from there and MatViewRefreshJob sizes its refresh interval from
        // the table's min/max, so leaving those behind would hide the row from the refresh entirely.
        // First record of a segment: WALE_HEADER_SIZE, record length (int), txn (long), txn type (byte),
        // start row id (long), end row id (long), then min timestamp and max timestamp.
        final long eventMinTimestampOffset = WalUtils.WALE_HEADER_SIZE + Integer.BYTES + Long.BYTES + Byte.BYTES + 2L * Long.BYTES;
        final long eventMaxTimestampOffset = eventMinTimestampOffset + Long.BYTES;
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot())
                    .concat(engine.verifyTableName(tableName))
                    .concat(WalUtils.WAL_NAME_BASE).put("1")
                    .concat("0");
            final int segmentLen = path.size();
            rewriteLongs(ff, path.concat("ts.d"), expected, replacement, timestampColumnOffset);
            rewriteLongs(
                    ff,
                    path.trimTo(segmentLen).concat(WalUtils.EVENT_FILE_NAME),
                    expected,
                    replacement,
                    eventMinTimestampOffset,
                    eventMaxTimestampOffset
            );
        }
    }

    /**
     * Asserts the end state both live-view tests share: the view is durably invalid and its reason
     * names the flush retry budget rather than the nano ceiling, nothing reached the view, and its
     * base table is untouched and still queryable. Drops the view last, so the enclosing
     * {@code assertMemoryLeak} does not take its reading with the view's resources still held.
     */
    private void assertLiveViewInvalidatedByLegacyNanoRow() throws Exception {
        // the refresh failure is durable, and the reason names the budget rather than the ceiling
        assertQuery("SELECT view_name, base_table_name, view_status, invalidation_reason FROM live_views()")
                .noRandomAccess()
                .noLeakCheck()
                .returns("""
                        view_name\tbase_table_name\tview_status\tinvalidation_reason
                        lv\tbase\tinvalid\tflush retry budget exhausted
                        """);
        // nothing reached the view: the ceiling rejects the row before the copier runs
        assertQuery("SELECT ts, x, rn FROM lv").noLeakCheck().timestamp("ts").expectSize().returns("""
                ts\tx\trn
                """);
        // an invalid view leaves its base alone, and both stay queryable
        assertQuery("SELECT ts, x FROM base").noLeakCheck().timestamp("ts").expectSize().returns("""
                ts\tx
                2262-01-01T00:00:00.000000000Z\t1
                """);
        execute("DROP LIVE VIEW lv");
    }

    /**
     * Seeds {@code legacy} with the single row the two offset-pruning tests query: a designated
     * timestamp 31 days above the nano ceiling, the on-disk state a pre-fix build produced.
     * {@link #testDesignatedNanosTimestampOutOfBoundsRowInvalidatesMatView()} explains why the WAL
     * segment rewrite is the only way to reach that state, and why the table is partitioned by DAY.
     */
    private void seedLegacyBandRow() throws Exception {
        execute("CREATE TABLE legacy (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO legacy VALUES (" + CommonUtils.MAX_TIMESTAMP + ", 1)");
        // close the WAL writer so the segment on disk is complete before the rewrite
        engine.releaseInactive();
        rewriteWalSegmentDesignatedTimestamp("legacy", CommonUtils.MAX_TIMESTAMP, LEGACY_BAND_NANO);
        drainWalQueue();

        assertQuery("SELECT ts, x FROM legacy").timestamp("ts").expectSize().returns("""
                ts\tx
                2262-02-01T00:00:00.000000000Z\t1
                """);
        // The forward shift overflows the long domain and lands ~10 days above its floor, far below
        // the bound both tests use. Pin it as a raw long: the wrapped instant sits close enough to
        // the floor that its rendered form is not worth depending on.
        assertQuery("SELECT dateadd('d', " + OFFSET_PRUNING_STRIDE_DAYS + ", ts)::LONG shifted, x FROM legacy")
                .expectSize()
                .returns("""
                        shifted\tx
                        -9222507273709551616\t1
                        """);
    }
}
