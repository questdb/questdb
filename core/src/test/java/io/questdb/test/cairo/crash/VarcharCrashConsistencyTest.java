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
 * Proves the harness reproduces the (already fixed) VARCHAR torn-aux bug: zeroing the last aux
 * entry's offset bytes via tornTail and reopening must NOT silently overwrite committed rows.
 * Green on HEAD (the setAppendPosition guard throws); RED on the pre-fix commit (silent overwrite).
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

            // queue torn-tail: zero bytes 8-15 of the last committed aux entry (the bug trigger)
            TableToken tt = engine.verifyTableName("v");
            try (Path aux = new Path()) {
                aux.of(engine.getConfiguration().getDbRoot()).concat(tt)
                        .concat(TableUtils.DEFAULT_PARTITION_NAME).slash();
                TableUtils.iFile(aux, "x", TableUtils.COLUMN_NAME_TXN_NONE);
                long base = (long) (rows - 1) * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                crashFf.tornTail(aux.$(), base + 8L, 8L);
            }

            crashAndReopen();

            // append drives setAppendPosition recovery -> guard must throw, not corrupt
            boolean detected = false;
            try {
                execute("insert into v values (" + (rows * 1_000_000L) + ", 'newrow" + SPLIT + "')");
            } catch (CairoException | CairoError e) {
                detected = true;
            }
            engine.releaseAllWriters();

            // row 0 must never be silently wrong, regardless of detection.
            // If the reader itself throws CairoException/CairoError (e.g., "varchar is outside of
            // file boundary") after the writer guard fired, that is also a loud-detection outcome —
            // still not silent corruption.
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
