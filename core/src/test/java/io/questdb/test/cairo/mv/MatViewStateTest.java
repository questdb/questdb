package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MatViewStateTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testMatViewNoStateFile() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym string, price double, ts timestamp" +
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
                        "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                                "price_1h\tincremental\tbase_price\t\t\tselect sym0, last(price0) price, ts0 from (select ts as ts0, sym as sym0, price as price0 from base_price) sample by 1h\tprice_1h~2\t\tvalid\t-1\t0\t0\t\t\t0\t\n",
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
            execute(
                    "create table base_price (" +
                            "  sym string, price double, ts timestamp" +
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
                        walWriter.resetMatViewState(i, i, true, "Invalidating " + i);
                        assertState(tableToken, i, i, true, "Invalidating " + i);
                    }
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putStr(0, "ABC");
                    row.putDouble(1, rnd.nextDouble());
                    row.append();
                    walWriter.commitMatView(i, i, 0, 1);
                }
                assertState(tableToken, iterations - 1, iterations - 1, false, null);

                fail.set(true);
                // all subsequent state updates should fail
                for (int i = 10; i < 2 * iterations; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putStr(0, "ABC");
                    row.putDouble(1, rnd.nextDouble());
                    row.append();
                    walWriter.commitMatView(i, i, 0, 1);
                    drainWalQueue();
                    assertState(tableToken, iterations - 1, iterations - 1, false, null);
                }

                walWriter.resetMatViewState(42, 42, true, "missed invalidation");
                drainWalQueue();
                assertState(tableToken, iterations - 1, iterations - 1, false, null);
            }
        });
    }

    @Test
    public void testMatViewTransactionBlockStateMaintenance() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute(
                    "create table base_price (" +
                            "  sym string, price double, ts timestamp" +
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
                        walWriter.commitMatView(i, i, 0, 1);
                    } else {
                        walWriter.commit();
                    }
                }
                // lastTxn = iterations - 1
                long lastMatViewTxn = (iterations - 1) - 1;
                assertState(tableToken, lastMatViewTxn, lastMatViewTxn, false, null);
            }
        });
    }

    private static void assertState(TableToken viewToken, long lastRefreshBaseTxn, long lastRefreshTimestamp, boolean invalid, String invalidationReason) {
        drainWalQueue();
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
            final MatViewStateReader viewState = new MatViewStateReader().of(reader, viewToken);
            assertEquals(invalid, viewState.isInvalid());
            assertEquals(lastRefreshBaseTxn, viewState.getLastRefreshBaseTxn());
            assertEquals(lastRefreshTimestamp, viewState.getLastRefreshTimestamp());
            TestUtils.assertEquals(invalidationReason, viewState.getInvalidationReason());
        }
    }
}
