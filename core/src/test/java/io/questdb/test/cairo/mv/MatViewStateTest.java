package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.mv.MatViewRefreshState;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Chars;
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

public class MatViewStateTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // override default to test copy
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
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
    public void testMatViewStateMaintenance() throws Exception {
        final int ITERS = 10;
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

        assertMemoryLeak(
                ff, () -> {
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
                        for (int i = 0; i < ITERS; i++) {
                            boolean invalidate = rnd.nextBoolean();
                            if (invalidate) {
                                walWriter.invalidate(i, i, invalidate, "Invalidating " + i);
                                checkState(tableToken, i, i, invalidate, "Invalidating " + i);
                            }
                            TableWriter.Row row = walWriter.newRow(0);
                            row.putStr(0, "ABC");
                            row.putDouble(1, rnd.nextDouble());
                            row.append();
                            walWriter.commitWithExtra(i, i);
                            checkState(tableToken, i, i, false, null);
                        }
                        checkState(tableToken, ITERS - 1, ITERS - 1, false, null);

                        fail.set(true);
                        // all subsequent state updates should fail
                        for (int i = 10; i < 2 * ITERS; i++) {
                            TableWriter.Row row = walWriter.newRow(0);
                            row.putStr(0, "ABC");
                            row.putDouble(1, rnd.nextDouble());
                            row.append();
                            walWriter.commitWithExtra(i, i);
                            drainWalQueue();
                            checkState(tableToken, ITERS - 1, ITERS - 1, false, null);
                        }

                        walWriter.invalidate(42, 42, true, "missed invalidation");
                        drainWalQueue();
                        checkState(tableToken, ITERS - 1, ITERS - 1, false, null);
                    }
                }
        );
    }

    private static void checkState(TableToken tableToken, long lastRefreshBaseTxn, long lastRefreshTimestamp, boolean invalid, String invalidationReason) {
        drainWalQueue();
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(path.of(configuration.getDbRoot()).concat(tableToken).concat(MatViewRefreshState.MAT_VIEW_STATE_FILE_NAME).$());
            final BlockFileReader.BlockCursor cursor = reader.getCursor();
            // Iterate through the block until we find the one we recognize.
            while (cursor.hasNext()) {
                final ReadableBlock block = cursor.next();
                if (block.type() != MatViewRefreshState.MAT_VIEW_STATE_FORMAT_MSG_TYPE) {
                    // Unknown block, skip.
                    continue;
                }
                boolean invalid0 = block.getBool(0);
                long lastRefreshBaseTxn0 = block.getLong(Byte.BYTES);
                //TODO: update state format
//                long lastRefreshTimestamp0 = block.getLong(Byte.BYTES + Long.BYTES);
                String invalidationReason0 = Chars.toString(block.getStr(Byte.BYTES + Long.BYTES));
                assertEquals(invalid, invalid0);
                assertEquals(lastRefreshBaseTxn, lastRefreshBaseTxn0);
//                assertEquals(lastRefreshTimestamp, lastRefreshTimestamp0);
                assertEquals(invalidationReason, invalidationReason0);
            }
        }
    }
}
