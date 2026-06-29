package io.questdb.test.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MatViewStateTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MatViewStateTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testMatViewNoStateFile() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym string, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select sym0, last(price0) price, ts0 " +
                    "from (select ts as ts0, sym as sym0, price as price0 from base_price) " +
                    "sample by 1h";

            execute("create materialized view price_1h as (" + viewSql + ") partition by DAY");
            drainWalQueue();
            TableToken tableToken = engine.verifyTableName("price_1h");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$();
                assertFalse(configuration.getFilesFacade().exists(path.$()));
                final long coldStart = timestampType.getDriver()
                        .fromMicros(MatViewState.COLD_START_GAP_THRESHOLD_MICROS);
                assertQuery("select * from materialized_views()")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_period_hi\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit\trefresh_avg_commit_nanos\trefresh_avg_scan_sample_nanos\trefresh_avg_scan_range_ts_units\trefresh_gap_threshold_ts_units\tbackfill_max_ts\n" +
                                "price_1h\timmediate\tbase_price\t\t\tselect sym0, last(price0) price, ts0 from (select ts as ts0, sym as sym0, price as price0 from base_price) sample by 1h\tprice_1h~2\t\tvalid\t\t-1\t0\t0\t\t\t\t0\t\t0\t\t0\t\t0\t0\t0\t" + coldStart + "\t\n");
            }
        });
    }

    @Test
    public void testMatViewStateMaintenance() throws Exception {
        final int iterations = 13;
        AtomicBoolean fail = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.containsAscii(name, "_event.i")) {
                    if (fail.get()) {
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym string, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select sym0, last(price0) price, ts0 " +
                    "from (select ts as ts0, sym as sym0, price as price0 from base_price) " +
                    "sample by 1h";

            execute("create materialized view price_1h as (" + viewSql + ") partition by DAY");
            drainWalQueue();
            TableToken tableToken = engine.verifyTableName("price_1h");
            Rnd rnd = new Rnd();
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < iterations; i++) {
                    boolean invalidate = rnd.nextBoolean();
                    if (invalidate) {
                        walWriter.resetMatViewState(i, i, true, "Invalidating " + i, Numbers.LONG_NULL, null, -1);
                        assertState(tableToken, i, i, true, "Invalidating " + i, Numbers.LONG_NULL, null, -1);
                    }
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putStr(0, "ABC");
                    row.putDouble(1, rnd.nextDouble());
                    row.append();
                    walWriter.commitMatView(i, i, i, 0, 1);
                }
                assertState(tableToken, iterations - 1, iterations - 1, false, null, iterations - 1, null, -1);

                fail.set(true);
                // all subsequent state updates should fail
                for (int i = 10; i < 2 * iterations; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putStr(0, "ABC");
                    row.putDouble(1, rnd.nextDouble());
                    row.append();
                    walWriter.commitMatView(i, i, i, 0, 1);
                    drainWalQueue();
                    assertState(tableToken, iterations - 1, iterations - 1, false, null, iterations - 1, null, -1);
                }

                walWriter.resetMatViewState(42, 42, true, "missed invalidation", Numbers.LONG_NULL, null, -1);
                drainWalQueue();
                assertState(tableToken, iterations - 1, iterations - 1, false, null, iterations - 1, null, -1);
            }
        });
    }

    @Test
    public void testMatViewStateResetRefreshIntervals() throws Exception {
        assertMemoryLeak(ff, () -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym string, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select sym0, last(price0) price, ts0 " +
                    "from (select ts as ts0, sym as sym0, price as price0 from base_price) " +
                    "sample by 1h";

            execute("create materialized view price_1h as (" + viewSql + ") partition by DAY");
            drainWalQueue();
            int iterations = 10;
            TableToken tableToken = engine.verifyTableName("price_1h");
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                final LongList intervals = new LongList();
                for (int i = 0; i < iterations; i++) {
                    intervals.add((long) i, i);
                    walWriter.resetMatViewState(i, i, false, null, i, intervals, i);
                    assertState(tableToken, i, i, false, null, i, intervals, i);
                }
            }
        });
    }

    @Test
    public void testMatViewTransactionBlockStateMaintenance() throws Exception {
        assertMemoryLeak(ff, () -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym string, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select sym0, last(price0) price, ts0 " +
                    "from (select ts as ts0, sym as sym0, price as price0 from base_price) " +
                    "sample by 1h";

            execute("create materialized view price_1h as (" + viewSql + ") partition by DAY");
            drainWalQueue();
            int iterations = 10;
            TableToken tableToken = engine.verifyTableName("price_1h");
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < iterations; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putStr(0, "ABC");
                    row.putDouble(1, 0.0);
                    row.append();
                    if (i % 2 == 0) {
                        walWriter.commitMatView(i, i, i, 0, 1);
                    } else {
                        walWriter.commit();
                    }
                }
                // lastTxn = iterations - 1
                long lastMatViewTxn = (iterations - 1) - 1;
                long lastPeriodHi = (iterations - 1) - 1;
                assertState(tableToken, lastMatViewTxn, lastMatViewTxn, false, null, lastPeriodHi, null, -1);
            }
        });
    }

    @Test
    public void testReusedReaderResetsStaleStateWhenLaterBlocksAbsent() throws Exception {
        // A reused MatViewStateReader must not carry the later-block fields (lastPeriodHi,
        // refreshIntervalsBaseTxn, refreshIntervals) from a previously-read view into a state file that
        // lacks those blocks. Pre-intervals-block _mv files (written before the intervals block was added)
        // exist on upgraded deployments and load via the of(BlockFileReader) fallback/checkpoint path, which
        // sets those fields only from the later blocks. Without a reset, a stale refreshIntervalsBaseTxn
        // carried over here corrupts the loaded view's refresh baseline (the next incremental refresh skips
        // the gap between the real and stale watermark). of(BlockFileReader) must reset first, the way its
        // of(MatViewDataInfo) / of(MatViewInvalidationInfo) siblings already fully repopulate.
        assertMemoryLeak(() -> {
            execute("create table base_tok (ts timestamp) timestamp(ts) partition by DAY WAL");
            final TableToken token = engine.verifyTableName("base_tok");
            final FilesFacade ff = configuration.getFilesFacade();
            final int commitMode = configuration.getCommitMode();

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot());
                final int rootLen = path.size();

                // File A -- a full state with a period hi and tracked intervals (all blocks present).
                final LongList intervals = new LongList();
                intervals.add(100L, 200L);
                path.trimTo(rootLen).concat("mv_full_state").$();
                ff.touch(path.$());
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    MatViewState.append(1_000L, 7L, false, null, 50L, intervals, 5L, Long.MIN_VALUE, Numbers.LONG_NULL, writer);
                }

                // File B -- a legacy state with ONLY the first block (no ts/period/intervals blocks).
                path.trimTo(rootLen).concat("mv_legacy_state").$();
                ff.touch(path.$());
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    final AppendableBlock block = writer.append();
                    MatViewState.appendState(10L, false, null, block);
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE);
                    writer.commit();
                }

                final MatViewStateReader reader = new MatViewStateReader();

                // Prime the reader from the full file: it now holds positive later-block fields.
                try (BlockFileReader br = new BlockFileReader(configuration)) {
                    path.trimTo(rootLen).concat("mv_full_state").$();
                    br.of(path.$());
                    reader.of(br, token);
                }
                assertEquals(5L, reader.getRefreshIntervalsBaseTxn());
                assertEquals(50L, reader.getLastPeriodHi());

                // Read the legacy file with the SAME reader: the absent blocks' fields must reset, not carry.
                try (BlockFileReader br = new BlockFileReader(configuration)) {
                    path.trimTo(rootLen).concat("mv_legacy_state").$();
                    br.of(path.$());
                    reader.of(br, token);
                }
                assertEquals("lastRefreshBaseTxn must come from the first block", 10L, reader.getLastRefreshBaseTxn());
                assertEquals("refreshIntervalsBaseTxn must reset when the intervals block is absent",
                        -1L, reader.getRefreshIntervalsBaseTxn());
                assertEquals("lastPeriodHi must reset when the period block is absent",
                        Numbers.LONG_NULL, reader.getLastPeriodHi());
                assertTrue("refreshIntervals must reset when the intervals block is absent",
                        reader.getRefreshIntervals() == null || reader.getRefreshIntervals().size() == 0);
            }
        });
    }

    private static void assertState(
            TableToken viewToken,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            boolean invalid,
            String invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn
    ) {
        drainWalQueue();
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
            final MatViewStateReader viewState = new MatViewStateReader().of(reader, viewToken);
            assertEquals(invalid, viewState.isInvalid());
            assertEquals(lastRefreshBaseTxn, viewState.getLastRefreshBaseTxn());
            assertEquals(lastRefreshTimestamp, viewState.getLastRefreshTimestampUs());
            TestUtils.assertEquals(invalidationReason, viewState.getInvalidationReason());
            assertEquals(lastPeriodHi, viewState.getLastPeriodHi());
            if (refreshIntervals != null) {
                TestUtils.assertEquals(refreshIntervals, viewState.getRefreshIntervals());
            } else {
                assertTrue(viewState.getRefreshIntervals() == null || viewState.getRefreshIntervals().size() == 0);
            }
            assertEquals(refreshIntervalsBaseTxn, viewState.getRefreshIntervalsBaseTxn());
        }
    }

    private void executeWithRewriteTimestamp(CharSequence sqlText) throws SqlException {
        sqlText = sqlText.toString().replaceAll("#TIMESTAMP", timestampType.getTypeName());
        engine.execute(sqlText, sqlExecutionContext);
    }
}
