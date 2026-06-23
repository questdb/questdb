package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.std.Files;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Quantifies P2: in SYNC mode the engine msyncs but never fsyncs, so a file extend's size is not
 * journaled and a crash after a committed transaction can lose it. This probe asserts the CURRENT
 * (buggy) behaviour — the just-committed grown rows are NOT all durable after crash (they are lost,
 * or the reopen detects a _txn/data mismatch and throws). SP2 (fdatasync-on-extend) will INVERT
 * this to assertSyncDurable(all rows present).
 *
 * <p>We use a minimal mmap-page size (one OS page = 4096 bytes on Linux) so the column data file
 * extends to a second page during the 200-row batch. The crash harness truncates grown files back
 * to their last-fsynced (baseline) size; because SYNC mode never calls fsync on the data files
 * (only msync), the extended portion is lost after crash. The _txn file retains the post-commit row
 * count in page-cache (it never grew in file size), so on reopen the engine detects a
 * _txn-vs-data size mismatch and throws — the "lostOrThrew" P2 signal.
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
        // force a file extend, which is the P2 durability gap: the new size is never fdatasynced.
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

                boolean lostOrThrew;
                try {
                    List<String> actual = readColumn("p", "s");
                    lostOrThrew = actual.size() < all.size();
                    // whatever survived must be a correct prefix (no silent garbage)
                    for (int i = 0; i < actual.size(); i++) {
                        Assert.assertEquals("surviving row " + i + " wrong", all.get(i), actual.get(i));
                    }
                } catch (CairoException | CairoError e) {
                    lostOrThrew = true; // reopen detected the torn/short files - also "not durable"
                } catch (InternalError e) {
                    // JVM converts SIGBUS (mmap access past truncated file end) to InternalError
                    lostOrThrew = true;
                } catch (RuntimeException e) {
                    // readColumn wraps SqlException in RuntimeException; unwrap CairoException/CairoError/InternalError
                    if (e.getCause() instanceof CairoException || e.getCause() instanceof CairoError
                            || e.getCause() instanceof InternalError) {
                        lostOrThrew = true;
                    } else {
                        throw e;
                    }
                }

                Assert.assertTrue(
                        "P2 probe: current SYNC must NOT durably keep the grown extent on crash "
                                + "(SP2 will invert this to zero-loss durability)",
                        lostOrThrew);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(2097152L));
        }
    }
}
