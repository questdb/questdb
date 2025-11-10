package io.questdb.test.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileReader;
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
                assertQueryNoLeakCheck(
                        """
                                view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_period_hi\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                                price_1h\timmediate\tbase_price\t\t\tselect sym0, last(price0) price, ts0 from (select ts as ts0, sym as sym0, price as price0 from base_price) sample by 1h\tprice_1h~2\t\tvalid\t\t-1\t0\t0\t\t\t\t0\t\t0\t\t0\t
                                """,
                        "select * from materialized_views()",
                        null
                );
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
