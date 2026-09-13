package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.std.Files;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Asserts B1 SYNC durability (SP2): after fsync-on-extend is applied in MemoryCMARWImpl and
 * MemoryPMARImpl, every row committed in SYNC mode must survive a simulated power-loss crash
 * (file extends are now inode-journaled via fsync). A crash after a SYNC commit must leave ALL
 * committed rows intact — zero loss.
 *
 * <p>We use a minimal mmap-page size (one OS page = 4096 bytes on Linux) so the column data file
 * extends to a second page during the batch. This forces the fsync-on-extend path we are
 * verifying. The crash harness truncates grown files back to their last-fsynced (baseline) size;
 * with B1 in place the fsync records the new inode size, so the harness preserves the extended
 * data after crash.
 */
public class Phase2DurabilityProbeTest extends AbstractCrashConsistencyTest {

    /**
     * One OS page in bytes. The STRING primary column (.d) stores 4 + 2*N bytes per value, so
     * 200 rows of 15-char strings ≈ 6 800 bytes — more than one page — guaranteeing at least one
     * file-extend call that the harness will roll back on crash.
     */
    private static final long MIN_PAGE = Files.PAGE_SIZE; // 4096 on Linux x86

    @Test
    public void testSyncCommitLosesExtendOnCrash_currentBehaviour() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // Use a minimal mmap page size so the extra rows overflow the pre-allocated segment and
        // force a file extend. With B1 (fsync-on-extend) the new inode size is journaled, so the
        // crash harness preserves all extended data.
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
                // boundary, even on platforms with large pages (16K/64K, e.g. Apple Silicon CI).
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

                // B1 makes the SYNC extend durable: ALL committed rows must survive the crash.
                assertSyncDurable("p", "s", all);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(2097152L));
        }
    }
}
