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
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.microtime.Micros;
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
     * The two halves multiply in different places. The {@code INSERT INTO ... SELECT} leaves the
     * cursor in micros, so {@code SqlCompilerImpl.copyOrderedBatched0()} - reached through
     * {@code InsertAsSelectOperationImpl} - finds a non-null converter
     * ({@code NanosTimestampDriver.getTimestampUnitConverter()} answers
     * {@code CommonUtils::microsToNanos} for a micros source) and converts on the way into
     * {@code newRow()}. The CTAS below carries an explicit {@code ts::TIMESTAMP_NS}, so its cursor
     * is already in nanos, its converter is null, and the projection has done the multiplication
     * before {@code copyOrderedBatched0()} ever sees the value. Either way {@code newRow()}
     * receives the same out-of-range nano timestamp and rejects it.
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
     * forward fails. {@code INSERT INTO ... SELECT} reaches the writer through
     * {@code InsertAsSelectOperationImpl}, which calls the public
     * {@code SqlCompilerImpl.copyOrderedBatched()}; {@code CREATE TABLE ... AS SELECT} reaches it
     * through the private {@code SqlCompilerImpl.copyTableData()}. Both land in
     * {@code copyOrderedBatched0()}, and because source and destination are both timestamp_ns its
     * unit converter is null, so both take the branch that hands {@code newRow()} the record's
     * timestamp as it stands. {@code MatViewRefreshJob} checks that timestamp only against the
     * replace range it is refreshing before passing it to {@code WalWriter.newRow()}, never against
     * the nano ceiling, so a view over such a table stops refreshing for the same reason.
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
     * A materialized view over a base table that a pre-fix build let an out-of-range row into stops
     * refreshing and goes durably invalid. {@code MatViewRefreshJob} hands every result row's
     * timestamp straight to {@code WalWriter.newRow(long)}, which calls
     * {@code NanosTimestampDriver.validateBounds()}. The {@code CairoException} that comes back is
     * neither of the two kinds {@code MatViewRefreshJob.isRetriableRefreshError()} defers
     * ({@code EntryUnavailableException} and a Cairo OOM), so the refresh takes the fail path, which
     * resets the view's WAL state with {@code invalid=true} and cascades to dependent views.
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
}
