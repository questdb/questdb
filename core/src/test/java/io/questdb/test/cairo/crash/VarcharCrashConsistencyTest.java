package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Checks that a torn last varchar aux entry is detected rather than applied: zeroing the offset bytes of the last
 * aux entry with tornTail and reopening must not silently overwrite committed rows. The setAppendPosition guard
 * throws on this layout.
 */
public class VarcharCrashConsistencyTest extends AbstractCrashConsistencyTest {

    private static final String SPLIT = "AAAABBBBCCCCDDDDEEEE"; // 20 bytes -> split (>9)

    @Test
    public void testTornLastAuxEntryNeverSilentlyCorrupts() throws Exception {
        runWithCrashFacade(() -> {
            final int rows = 10;
            execute("create table v (ts timestamp, x varchar) timestamp(ts) partition by none");
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                String val = "row" + String.format("%02d", i) + SPLIT;
                execute("insert into v values (" + (i * 1_000_000L) + ", '" + val + "')");
                expected.add(val);
            }
            markDurableBaseline();

            // queue a torn tail: zero bytes 8-15 of the last committed aux entry
            TableToken tt = engine.verifyTableName("v");
            try (Path aux = new Path()) {
                aux.of(engine.getConfiguration().getDbRoot()).concat(tt)
                        .concat(TableUtils.DEFAULT_PARTITION_NAME).slash();
                TableUtils.iFile(aux, "x", TableUtils.COLUMN_NAME_TXN_NONE);
                long base = (long) (rows - 1) * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                crashFf.tornTail(aux.$(), base + 8L, 8L);
            }

            crashAndReopen();

            // the append drives setAppendPosition recovery; the guard must throw, not corrupt
            boolean detected = false;
            try {
                execute("insert into v values (" + (rows * 1_000_000L) + ", 'newrow" + SPLIT + "')");
            } catch (CairoException | CairoError e) {
                detected = true;
            }
            engine.releaseAllWriters();

            // row 0 must never be silently wrong, whether or not the writer detected the tear. A reader
            // CairoException or CairoError (e.g. "varchar is outside of file boundary") also counts as detection.
            try {
                List<String> actual = readVarcharColumn("v", "x");
                if (!actual.isEmpty()) {
                    Assert.assertEquals("row 0 silently corrupted", expected.get(0), actual.get(0));
                }
            } catch (CairoException | CairoError e) {
                // loud detection on the read path: acceptable, not silent corruption
            } catch (RuntimeException e) {
                // readVarcharColumn wraps SqlException in RuntimeException; unwrap and allow
                // CairoException/CairoError causes (same loud-detection path)
                if (!(e.getCause() instanceof CairoException) && !(e.getCause() instanceof CairoError)) {
                    throw e;
                }
            }
            Assert.assertTrue("torn last aux entry must be detected on reopen", detected);
        });
    }
}
