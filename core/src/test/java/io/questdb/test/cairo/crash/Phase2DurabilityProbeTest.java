package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.std.Files;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Checks that every row committed in SYNC mode survives a simulated power loss when the commit
 * extends a column file. MemoryCMARWImpl and MemoryPMARImpl fsync on extend, which journals the
 * new inode size.
 *
 * <p>The test uses a one-page append size so the column data file grows past its first page
 * during the batch and takes the fsync-on-extend path. The crash harness truncates grown files
 * back to their last fsynced size, so a missing fsync on extend loses rows.
 */
public class Phase2DurabilityProbeTest extends AbstractCrashConsistencyTest {

    /**
     * One OS page in bytes, used as the append page size so the string column's data file
     * extends during the batch.
     */
    private static final long MIN_PAGE = Files.PAGE_SIZE; // 4096 on Linux x86

    @Test
    public void testSyncCommitLosesExtendOnCrash_currentBehaviour() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // Use a minimal mmap page size so the extra rows overflow the pre-allocated segment and
        // force a file extend.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(MIN_PAGE));
        try {
            // engine.getConfiguration() live-delegates to the property store updated above, so
            // this assert is valid before runWithCrashFacade starts the engine under the facade.
            Assert.assertEquals("probe requires SYNC commit mode",
                    CommitMode.SYNC, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                execute("create table p (ts timestamp, s string) timestamp(ts) partition by none");
                execute("insert into p values (0, 'seed-row-000000')");
                markDurableBaseline(); // the seed is "old, already journaled"

                // Scale row count to page size so the .d column file always crosses a page
                // boundary, even on platforms with 16K or 64K pages.
                // STRING data cost: 4-byte length prefix + 2 bytes per UTF-16 char.
                final long pageSize = MIN_PAGE;
                final String VAL = "grow-row-row-row-row"; // 20 chars; last 6 replaced per row
                final int bytesPerRow = 4 + 2 * VAL.length(); // 4 + 40 = 44 bytes/row
                final int extra = (int) Math.max(200, (3L * pageSize) / bytesPerRow + 1);
                List<String> all = new ArrayList<>();
                all.add("seed-row-000000");
                for (int i = 1; i <= extra; i++) {
                    // Replace last 6 chars with zero-padded index to keep value length constant.
                    String v = VAL.substring(0, VAL.length() - 6) + String.format("%06d", i);
                    execute("insert into p values (" + (i * 1_000_000L) + ", '" + v + "')");
                    all.add(v);
                }

                crashAndReopen();

                // fsync on extend makes the SYNC commit durable: all committed rows must survive the crash.
                assertSyncDurable("p", "s", all);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(2097152L));
        }
    }
}
