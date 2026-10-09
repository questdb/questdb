package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.ColumnType.LEGACY_VAR_SIZE_AUX_SHL;

/**
 * Pins the writer's behaviour when several trailing aux entries of a string or array
 * column are zeroed: the append after reopen throws nothing.
 * <p>
 * A guard in a driver's setAppendPosition of the form
 * <pre>
 *   long dataAppendOffset = dataMem.getAppendOffset();
 *   if (dataVectorSize == 0 &amp;&amp; dataAppendOffset &gt; 0) { throw ... }
 * </pre>
 * cannot catch this layout. dataMem is a MemoryPMARImpl opened via openColumnFiles() ->
 * MemoryPMARImpl.of() -> close() + setExtendSegmentSize(). setExtendSegmentSize() calls
 * clear(), which sets appendPointer=-1 and baseOffset=1, so getAppendOffset() returns 0,
 * and nothing calls ff.length() or jumpTo() before setAppendPosition runs. The condition
 * is therefore always false, whatever the data file's actual size.
 * <p>
 * The tests zero ZEROED_ENTRY_COUNT consecutive aux entries so that both the last and
 * second-to-last entries read 0, which also passes the single-entry monotonicity guard
 * (0 &gt;= 0). Varchar is tested separately: a debug-only assert 'raw != 0' in
 * getDataVectorSize fires on zeroed entries in test builds.
 */
public class MultiPageTornAuxTailTest extends AbstractCrashConsistencyTest {

    /**
     * Number of consecutive aux entries to zero (simulates multi-entry tail loss).
     */
    private static final int ZEROED_ENTRY_COUNT = 4;

    @Test
    public void testStringMultiPageTornAuxTailGuardIsIneffective() throws Exception {
        // A long string to ensure real data in the data file (>0 bytes).
        final String longValue = "AAAABBBBCCCCDDDDEEEE"; // 20 chars
        runWithCrashFacade(() -> {
            final int rows = 20;
            execute("create table s (ts timestamp, x string) timestamp(ts) partition by none");

            for (int i = 0; i < rows; i++) {
                execute("insert into s values (" + (i * 1_000_000L) + ", '" + longValue + i + "')");
            }
            markDurableBaseline();

            // Zero the last ZEROED_ENTRY_COUNT aux entries.
            // STRING aux entries are 8 bytes each (1 << LEGACY_VAR_SIZE_AUX_SHL = 8).
            // Zeroing 4 entries means both entry[pos-1] and entry[pos-2] read 0,
            // so the single-entry monotonicity guard also passes (0 >= 0).
            // The data file still has all real data (data vector unchanged).
            final int stringAuxEntryBytes = 1 << LEGACY_VAR_SIZE_AUX_SHL; // 8 bytes
            TableToken tt = engine.verifyTableName("s");
            try (Path aux = new Path()) {
                aux.of(engine.getConfiguration().getDbRoot())
                        .concat(tt)
                        .concat(TableUtils.DEFAULT_PARTITION_NAME)
                        .slash();
                TableUtils.iFile(aux, "x", TableUtils.COLUMN_NAME_TXN_NONE);
                long firstZeroedEntry = (long) (rows - ZEROED_ENTRY_COUNT) * stringAuxEntryBytes;
                long zeroLen = (long) ZEROED_ENTRY_COUNT * stringAuxEntryBytes;
                crashFf.tornTail(aux.$(), firstZeroedEntry, zeroLen);
            }

            crashAndReopen();

            // A guard on dataMem.getAppendOffset() before jumpTo cannot fire: that value is
            // always 0 on a freshly opened MemoryPMARImpl, so (0 == 0 && 0 > 0) is never true.
            boolean exceptionThrown = false;
            try {
                execute("insert into s values (" + (rows * 1_000_000L) + ", '" + longValue + "')");
            } catch (CairoException | CairoError e) {
                exceptionThrown = true;
            } finally {
                engine.releaseAllWriters();
            }

            // no exception: an append-offset guard could not have caught this layout
            Assert.assertFalse(
                    "STRING: an exception was thrown, meaning some guard DID fire. " +
                            "If it's the multi-page guard, the DROP decision is wrong — re-investigate.",
                    exceptionThrown
            );
        });
    }

    @Test
    public void testArrayMultiPageTornAuxTailGuardIsIneffective() throws Exception {
        runWithCrashFacade(() -> {
            final int rows = 20;
            execute("create table a (ts timestamp, arr double[]) timestamp(ts) partition by none");

            for (int i = 0; i < rows; i++) {
                execute("insert into a values (" + (i * 1_000_000L) + ", ARRAY[1.0,2.0,3.0])");
            }
            markDurableBaseline();

            // Zero the last ZEROED_ENTRY_COUNT aux entries for ARRAY (16 bytes each).
            // Both entry[pos-1] and entry[pos-2] read 0, so monotonicity guard passes (0 >= 0).
            TableToken tt = engine.verifyTableName("a");
            try (Path aux = new Path()) {
                aux.of(engine.getConfiguration().getDbRoot())
                        .concat(tt)
                        .concat(TableUtils.DEFAULT_PARTITION_NAME)
                        .slash();
                TableUtils.iFile(aux, "arr", TableUtils.COLUMN_NAME_TXN_NONE);
                long firstZeroedEntry = (long) (rows - ZEROED_ENTRY_COUNT) * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                long zeroLen = (long) ZEROED_ENTRY_COUNT * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                crashFf.tornTail(aux.$(), firstZeroedEntry, zeroLen);
            }

            crashAndReopen();

            // As for strings, dataMem.getAppendOffset() is 0 before jumpTo, so an append-offset guard cannot fire.
            boolean exceptionThrown = false;
            try {
                execute("insert into a values (" + (rows * 1_000_000L) + ", ARRAY[9.0,9.0,9.0])");
            } catch (CairoException | CairoError e) {
                exceptionThrown = true;
            } finally {
                engine.releaseAllWriters();
            }

            // no exception: an append-offset guard could not have caught this layout
            Assert.assertFalse(
                    "ARRAY: an exception was thrown, meaning some guard DID fire. " +
                            "If it's the multi-page guard, the DROP decision is wrong — re-investigate.",
                    exceptionThrown
            );
        });
    }
}
